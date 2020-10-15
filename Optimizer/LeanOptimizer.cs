/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using System;
using System.Linq;
using QuantConnect.Util;
using Newtonsoft.Json.Linq;
using QuantConnect.Logging;
using System.Collections.Concurrent;

namespace QuantConnect.Optimizer
{
    /// <summary>
    /// Base Lean optimizer class in charge of handling an optimization job packet
    /// TODO: should allow optionally limiting the amount of parallel executions
    /// </summary>
    public abstract class LeanOptimizer : IDisposable
    {
        /// <summary>
        /// Collection holding <see cref="ParameterSet"/> for each backtest id we are waiting to finish
        /// </summary>
        protected readonly ConcurrentDictionary<string, ParameterSet> ParameterSetForBacktest;

        /// <summary>
        /// The optimization strategy being used
        /// </summary>
        protected readonly IOptimizationStrategy Strategy;

        /// <summary>
        /// The optimization packet
        /// </summary>
        protected readonly OptimizationNodePacket NodePacket;

        /// <summary>
        /// Event triggered when the optimization work ended
        /// </summary>
        public event EventHandler Ended;

        /// <summary>
        /// Creates a new instance
        /// </summary>
        /// <param name="nodePacket">The optimization node packet to handle</param>
        protected LeanOptimizer(OptimizationNodePacket nodePacket)
        {
            if (nodePacket.OptimizationParameters.IsNullOrEmpty())
            {
                throw new InvalidOperationException("Cannot start an optimization job with no parameter to optimize");
            }

            NodePacket = nodePacket;
            Strategy =
                Composer.Instance.GetExportedValueByTypeName<IOptimizationStrategy>(NodePacket.OptimizationStrategy);

            ParameterSetForBacktest = new ConcurrentDictionary<string, ParameterSet>();

            Strategy.Initialize(
                Composer.Instance.GetExportedValueByTypeName<IOptimizationParameterSetGenerator>(NodePacket
                    .ParameterSetGenerator),
                NodePacket.Criterion["extremum"] == "max"
                    ? new Maximization() as Extremum
                    : new Minimization(),
                NodePacket.OptimizationParameters);

            Strategy.NewParameterSet += (s, e) =>
            {
                var paramSet = (e as OptimizationEventArgs)?.ParameterSet;
                if (paramSet == null) return;

                lock (ParameterSetForBacktest)
                {
                    try
                    {
                        var backtestId = RunLean(paramSet);

                        if (!string.IsNullOrEmpty(backtestId))
                        {
                            ParameterSetForBacktest.TryAdd(backtestId, paramSet);
                        }
                        else
                        {
                            Log.Error("LeanOptimizer.NewParameterSet(): Empty/null optimization compute job could not be placed into the queue");
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Error($"LeanOptimizer.NewParameterSet(): Error encountered while placing optimization message into the queue: {ex.Message}");
                    }
                }
            };
        }

        /// <summary>
        /// Starts the optimization
        /// </summary>
        public virtual void Start()
        {
            Strategy.PushNewResults(OptimizationResult.Empty);
        }

        /// <summary>
        /// Triggers the optimization job end event
        /// </summary>
        protected virtual void TriggerOnEndEvent(EventArgs eventArgs)
        {
            Ended?.Invoke(this, eventArgs);
        }

        /// <summary>
        /// Handles starting Lean for a given parameter set
        /// </summary>
        /// <param name="parameterSet">The parameter set for the backtest to run</param>
        /// <returns>The new unique backtest id</returns>
        protected abstract string RunLean(ParameterSet parameterSet);

        /// <summary>
        /// Handles a new backtest json result matching a requested backtest id
        /// </summary>
        /// <param name="jsonBacktestResult">The backtest json result</param>
        /// <param name="backtestId">The associated backtest id</param>
        protected virtual void NewResult(string jsonBacktestResult, string backtestId)
        {
            OptimizationResult result;
            ParameterSet parameterSet;

            // we take a lock so that there is no race condition with launching Lean adding the new backtest id and receiving the backtest result for that id
            // before it's even in the collection 'ParameterSetForBacktest'
            lock (ParameterSetForBacktest)
            {
                if (!ParameterSetForBacktest.TryRemove(backtestId, out parameterSet))
                {
                    Log.Error($"LeanOptimizer.NewResult(): Optimization compute job with id '{backtestId}' was not found");
                    return;
                }
            }

            if (string.IsNullOrEmpty(jsonBacktestResult))
            {
                Log.Error($"LeanOptimizer.NewResult(): Got null/empty backtest result for backtest id '{backtestId}'");
                Strategy.PushNewResults(null);
            }
            else
            {
                var value = JObject.Parse(jsonBacktestResult).SelectToken(NodePacket.Criterion["name"]).Value<decimal>();

                result = new OptimizationResult(value, parameterSet);

                Strategy.PushNewResults(result);
            }

            if (!ParameterSetForBacktest.Any())
            {
                // TODO: could send winning backtest id/result?
                TriggerOnEndEvent(EventArgs.Empty);
            }
        }

        /// <summary>
        /// Disposes of any resources
        /// </summary>
        public abstract void Dispose();
    }
}
