using NodaTime.Extensions;
using NodaTime;
using ShardingCore.VirtualRoutes.Abstractions;

namespace ShardingCore.Abstractions.VirtualRoutes
{
    public abstract class AbstractShardingTimeKeyInstantVirtualTableRoute<TEntity> : AbstractShardingAutoCreateOperatorVirtualTableRoute<TEntity, Instant> where TEntity : class
    {
        /// <summary>
        /// 分片开始时间请使用固定值 eg.new DateTime(20xx,xx,xx)
        /// 固定值的意思就是每次程序启动这个值都不会变化，如果你使用了Datetime.Now那么程序每次
        /// 启动获取到的这个值都是动态的是不正确的所以需要你返回一个固定值，
        /// 这个方法仅在启动时被框架调用一次用于计算
        /// </summary>
        /// <returns></returns>
        public abstract DateTime GetBeginTime();


        /// <summary>
        /// how convert sharding key to tail
        /// </summary>
        /// <param name="shardingKey"></param>
        /// <returns></returns>
        public override string ShardingKeyToTail(object shardingKey)
        {
            return shardingKey switch
            {
                Instant instant => TimeFormatToTail(instant),
                _ => throw new ArgumentException("shardingKey is error"),
            };
        }
        /// <summary>
        /// how format date time to tail
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        protected abstract string TimeFormatToTail(Instant time);

        protected override string ConvertNowToTail(DateTime now)
        {
            return ShardingKeyToTail(now.ToInstant());
        }
    }
}
