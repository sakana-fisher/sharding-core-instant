using NodaTime.Extensions;
using ShardingCore.VirtualRoutes.Abstractions;

namespace ShardingCore.Abstractions.VirtualRoutes
{
    public abstract class AbstractShardingInstantKeyLongVirtualTableRoute<TEntity> : AbstractShardingAutoCreateOperatorVirtualTableRoute<TEntity, long> where TEntity : class
    {
        public abstract DateTime GetBeginTime();

        /// <summary>
        /// how convert sharding key to tail
        /// </summary>
        /// <param name="shardingKey"></param>
        /// <returns></returns>
        public override string ShardingKeyToTail(object shardingKey)
        {
            var time = (long)shardingKey;
            return TimeFormatToTail(time);
        }
        /// <summary>
        /// how format long time to tail
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        protected abstract string TimeFormatToTail(long time);

        protected override string ConvertNowToTail(DateTime now)
        {
            return ShardingKeyToTail(now.ToInstant().ToUnixTimeMilliseconds());
        }
    }

}
