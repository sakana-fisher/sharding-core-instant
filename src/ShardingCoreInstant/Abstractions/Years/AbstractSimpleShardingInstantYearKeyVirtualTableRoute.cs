using NodaTime;
using ShardingCore.Abstractions.VirtualRoutes;
using ShardingCore.Core.VirtualRoutes;

namespace ShardingCore.Abstractions.Years
{
    public abstract class AbstractSimpleShardingInstantYearKeyVirtualTableRoute<TEntity> : AbstractShardingTimeKeyInstantVirtualTableRoute<TEntity> where TEntity : class
    {
        protected override List<string> CalcTailsOnStart()
        {
            var beginTime = new DateTime(GetBeginTime().Year, 1, 1);

            var tails = new List<string>();
            //提前创建表
            var now = DateTime.Now;
            var nowTimeStamp = new DateTime(now.Year, 1, 1);
            if (beginTime > nowTimeStamp)
                throw new ArgumentException("begin time error");
            var currentTimeStamp = beginTime;
            while (currentTimeStamp <= nowTimeStamp)
            {
                var tail = ShardingKeyToTail(currentTimeStamp);
                tails.Add(tail);
                currentTimeStamp = currentTimeStamp.AddYears(1);
            }
            return tails;
        }
        protected override string TimeFormatToTail(Instant time)
        {
            var date = time.ToDateTimeUtc().ToLocalTime();

            return $"{date:yyyy}";
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
                        var date = shardingKey.ToDateTimeUtc().ToLocalTime();
                        var currentYear = new DateTime(date.Year, 1, 1);
                        //处于临界值 o=>o.time < [2021-01-01 00:00:00] 尾巴20210101不应该被返回
                        if (currentYear == date)
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
        /// <summary>
        /// 在几时执行创建对应的表
        /// </summary>
        /// <returns></returns>
        public override string[] GetCronExpressions()
        {
            return new[]
            {
                "0 59 23 31 12 ?",
                "0 0 0 1 1 ?",
                "0 1 0 1 1 ?",
            };
        }
        public override string[] GetJobCronExpressions()
        {
            var crons = base.GetJobCronExpressions().Concat(new[] { "0 0 0 1 1 ?" }).Distinct().ToArray();
            return crons;
        }
    }
}
