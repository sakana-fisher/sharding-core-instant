﻿using ShardingCore.Abstractions.VirtualRoutes;
using ShardingCore.Core.VirtualRoutes;
using ShardingCore.Helpers;
using ShardingCore.VirtualRoutes.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShardingCore.Abstractions.Years
{
    public abstract class AbstractSimpleShardingInstantYearKeyLongVirtualTableRoute<TEntity> : AbstractShardingInstantKeyLongVirtualTableRoute<TEntity> where TEntity : class
    {
        /// <summary>
        /// 返回这个对象在数据库里面的所有表后缀
        /// </summary>
        /// <returns></returns>
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
                var currentTimeStampLong = ShardingCoreHelper.ConvertDateTimeToLong(currentTimeStamp);
                var tail = ShardingKeyToTail(currentTimeStampLong);
                tails.Add(tail);
                currentTimeStamp = currentTimeStamp.AddYears(1);
            }

            return tails;
        }
        /// <summary>
        /// 如何将时间转换成后缀
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        protected override string TimeFormatToTail(long time)
        {
            var date = ShardingCoreHelper.ConvertLongToDateTime(time);
            return $"{date:yyyy}";
        }
        /// <summary>
        /// 当where条件用到对应的值时会调用改方法
        /// </summary>
        /// <param name="shardingKey"></param>
        /// <param name="shardingOperator"></param>
        /// <returns>当传入表后缀你告诉框架这个后缀是否需要被返回，分片字段如何筛选出后缀</returns>
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
                        var datetime = ShardingCoreHelper.ConvertLongToDateTime(shardingKey);
                        var currentYear = new DateTime(datetime.Year, 1, 1);
                        //处于临界值 o=>o.time < [2021-01-01 00:00:00] 尾巴20210101不应该被返回
                        if (currentYear == datetime)
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
        /// 在几时创建对应的表
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
