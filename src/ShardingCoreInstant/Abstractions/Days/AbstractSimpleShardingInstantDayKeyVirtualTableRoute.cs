﻿using NodaTime;
using ShardingCore.Abstractions.VirtualRoutes;
using ShardingCore.Core.VirtualRoutes;

namespace ShardingCore.Abstractions.Days
{
    public abstract class AbstractSimpleShardingInstantDayKeyVirtualTableRoute<TEntity> :
            AbstractShardingTimeKeyInstantVirtualTableRoute<TEntity> where TEntity : class
    {


        /// <summary>
        /// 这个方法会在程序启动的时候被调用,后续整个生命周期将不会被调用,仅用来告诉框架启动的时候有多少张TEntity对象的后缀表,
        /// 然后会在启动的时候添加到 <see cref="AbstractShardingAutoCreateOperatorVirtualTableRoute{TEntity}._tails"/>
        /// </summary>
        /// <returns></returns>
        protected override List<string> CalcTailsOnStart()
        {
            var beginTime = GetBeginTime();

            var tails = new List<string>();
            //提前创建表
            var nowTimeStamp = DateTime.Now.Date;
            if (beginTime > nowTimeStamp)
                throw new ArgumentException("begin time error");
            var currentTimeStamp = beginTime;
            while (currentTimeStamp <= nowTimeStamp)
            {
                var tail = ShardingKeyToTail(currentTimeStamp);
                tails.Add(tail);
                currentTimeStamp = currentTimeStamp.AddDays(1);
            }

            return tails;
        }

        protected override string TimeFormatToTail(Instant time)
        {
            var date = time.ToDateTimeUtc().ToLocalTime();
            return $"{date:yyyMMdd}";
        }

        public override Func<string, bool> GetRouteToFilter(Instant shardingKey, ShardingOperatorEnum shardingOperator)
        {
            var t = TimeFormatToTail(shardingKey);
            switch (shardingOperator)
            {
                case ShardingOperatorEnum.GreaterThan:
                case ShardingOperatorEnum.GreaterThanOrEqual:
                    return tail => String.Compare(tail, t, StringComparison.Ordinal) >= 0;
                case ShardingOperatorEnum.LessThan:
                    {
                        var shardingKeyDate = shardingKey;
                        //处于临界值 o=>o.time < [2021-01-01 00:00:00] 尾巴20210101不应该被返回
                        if (shardingKeyDate == shardingKey)
                            return tail => String.Compare(tail, t, StringComparison.Ordinal) < 0;
                        return tail => String.Compare(tail, t, StringComparison.Ordinal) <= 0;
                    }
                case ShardingOperatorEnum.LessThanOrEqual:
                    return tail => String.Compare(tail, t, StringComparison.Ordinal) <= 0;
                case ShardingOperatorEnum.Equal: return tail => tail == t;
                default:
                    {
#if DEBUG
                        Console.WriteLine($"shardingOperator is not equal scan all table tail");
#endif
                        return tail => true;
                    }
            }
        }

        public override string[] GetCronExpressions()
        {
            return new[]
            {
                "0 59 23 * * ?",
                "0 0 0 * * ?",
                "0 1 0 * * ?",
            };
        }

        public override string[] GetJobCronExpressions()
        {
            var crons = base.GetJobCronExpressions().Concat(new[] { "0 0 0 * * ?" }).Distinct().ToArray();
            return crons;
        }
    }
}
