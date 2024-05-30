using NodaTime;
using ShardingCore.Abstractions.VirtualRoutes;
using ShardingCore.Core.VirtualRoutes;
using ShardingCore.Helpers;

namespace ShardingCore.Abstractions.Weeks
{
    public abstract class AbstractSimpleShardingInstantWeekKeyVirtualTableRoute<TEntity> : AbstractShardingTimeKeyInstantVirtualTableRoute<TEntity> where TEntity : class
    {
        protected override List<string> CalcTailsOnStart()
        {
            var beginTime = ShardingCoreHelper.GetCurrentMonday(GetBeginTime()).Date;

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
                currentTimeStamp = currentTimeStamp.AddDays(7);
            }
            return tails;
        }
        protected override string TimeFormatToTail(Instant time)
        {
            var date = time.ToDateTimeUtc().ToLocalTime();
            var currentMonday = ShardingCoreHelper.GetCurrentMonday(date);
            var currentSunday = ShardingCoreHelper.GetCurrentSunday(date);
            return $"{currentMonday:yyyyMM}{currentMonday:dd}_{currentSunday:dd}";
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
                        var currentMonth = ShardingCoreHelper.GetCurrentMonday(dateTime);
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
                "0 59 23 ? * 1",
                "0 0 0 ? * 2",
                "0 1 0 ? * 2",
            };
        }
        public override string[] GetJobCronExpressions()
        {
            var crons = base.GetJobCronExpressions().Concat(new[] { "0 0 0 ? * 2" }).Distinct().ToArray();
            return crons;
        }

    }
}
