using NodaTime;
using ShardingCore.Abstractions.VirtualRoutes;
using ShardingCore.Core.VirtualRoutes;
using ShardingCore.Helpers;

namespace ShardingCore.Abstractions.Months
{
    public abstract class AbstractSimpleShardingInstantMonthKeyVirtualTableRoute<TEntity> : AbstractShardingTimeKeyInstantVirtualTableRoute<TEntity> where TEntity : class
    {
        protected override List<string> CalcTailsOnStart()
        {
            var beginTime = ShardingCoreHelper.GetCurrentMonthFirstDay(GetBeginTime());

            var tails = new List<string>();
            //提前创建表
            var nowTimeStamp = ShardingCoreHelper.GetCurrentMonthFirstDay(DateTime.Now);
            if (beginTime > nowTimeStamp)
                throw new ArgumentException("begin time error");
            var currentTimeStamp = beginTime;
            while (currentTimeStamp <= nowTimeStamp)
            {
                var tail = ShardingKeyToTail(currentTimeStamp);
                tails.Add(tail);
                currentTimeStamp = ShardingCoreHelper.GetNextMonthFirstDay(currentTimeStamp);
            }
            return tails;
        }
        protected override string TimeFormatToTail(Instant time)
        {
            var date = time.ToDateTimeUtc().ToLocalTime();
            return $"{date:yyyyMM}";
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
                        var dateTime = shardingKey.ToDateTimeUtc().ToLocalTime();

                        var currentMonth = ShardingCoreHelper.GetCurrentMonthFirstDay(dateTime);
                        //处于临界值 o=>o.time < [2021-01-01 00:00:00] 尾巴20210101不应该被返回
                        if (currentMonth == dateTime)
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
                "0 59 23 28,29,30,31 * ?",
                "0 0 0 1 * ?",
                "0 1 0 1 * ?",
            };
        }
        public override string[] GetJobCronExpressions()
        {
            var crons = base.GetJobCronExpressions().Concat(new[] { "0 0 0 1 * ?" }).Distinct().ToArray();
            return crons;
        }

    }
}
