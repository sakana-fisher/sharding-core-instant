using ShardingCore.Abstractions.VirtualRoutes;
using ShardingCore.Core.VirtualRoutes;
using ShardingCore.Helpers;

namespace ShardingCore.Abstractions.Days
{
    public abstract class AbstractSimpleShardingInstantDayKeyLongVirtualTableRoute<TEntity> : AbstractShardingInstantKeyLongVirtualTableRoute<TEntity> where TEntity : class
    {
        protected override List<string> CalcTailsOnStart()
        {
            var beginTime = GetBeginTime().Date;

            var tails = new List<string>();
            //提前创建表
            var nowTimeStamp = DateTime.Now.Date;
            if (beginTime > nowTimeStamp)
                throw new ArgumentException("begin time error");
            var currentTimeStamp = beginTime;
            while (currentTimeStamp <= nowTimeStamp)
            {
                var currentTimeStampLong = ShardingCoreHelper.ConvertDateTimeToLong(currentTimeStamp);
                var tail = TimeFormatToTail(currentTimeStampLong);
                tails.Add(tail);
                currentTimeStamp = currentTimeStamp.AddDays(1);
            }
            return tails;
        }
        protected override string TimeFormatToTail(long time)
        {
            var date = ShardingCoreHelper.ConvertLongToDateTime(time);
            return $"{date:yyyyMMdd}";
        }

        public override Func<string, bool> GetRouteToFilter(long shardingKey, ShardingOperatorEnum shardingOperator)
        {
            var t = TimeFormatToTail(shardingKey);
            switch (shardingOperator)
            {
                case ShardingOperatorEnum.GreaterThan:
                case ShardingOperatorEnum.GreaterThanOrEqual:
                    return tail => String.Compare(tail, t, StringComparison.Ordinal) >= 0;
                case ShardingOperatorEnum.LessThan:
                    {
                        var dateTime = ShardingCoreHelper.ConvertLongToDateTime(shardingKey);
                        var shardingKeyDate = dateTime.Date;
                        //处于临界值 o=>o.time < [2021-01-01 00:00:00] 尾巴20210101不应该被返回
                        if (shardingKeyDate == dateTime)
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
