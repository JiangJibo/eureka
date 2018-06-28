package com.netflix.eureka.registry.rule;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.eureka.lease.Lease;

import java.util.ArrayList;
import java.util.List;

/**
 * This rule takes an ordered list of rules and returns the result of the first match or the
 * result of the {@link AlwaysMatchInstanceStatusRule}.
 *
 * Created by Nikos Michalakis on 7/13/16.
 */
public class FirstMatchWinsCompositeRule implements InstanceStatusOverrideRule {

    /**
     * 复合规则集合
     * {@link DownOrStartingRule} : 若拉取的InstanceInfo状态是 DOWN 和 STARTING, 则匹配
     * {@link OverrideExistsRule}
     * {@link LeaseExistsRule}
     */
    private final InstanceStatusOverrideRule[] rules;
    /**
     * 默认规则 {@link AlwaysMatchInstanceStatusRule}
     */
    private final InstanceStatusOverrideRule defaultRule;
    private final String compositeRuleName;

    public FirstMatchWinsCompositeRule(InstanceStatusOverrideRule... rules) {
        this.rules = rules;
        this.defaultRule = new AlwaysMatchInstanceStatusRule();
        // Let's build up and "cache" the rule name to be used by toString();
        List<String> ruleNames = new ArrayList<>(rules.length + 1);
        for (int i = 0; i < rules.length; ++i) {
            ruleNames.add(rules[i].toString());
        }
        ruleNames.add(defaultRule.toString());
        compositeRuleName = ruleNames.toString();
    }

    /**
     * @param instanceInfo  The instance info whose status we care about. 关注状态的应用实例对象
     * @param existingLease Does the instance have an existing lease already? If so let's consider that. 已存在的租约
     * @param isReplication When overriding consider if we are under a replication mode from other servers. 是否是 Eureka-Server 发起的请求
     * @return
     */
    @Override
    public StatusOverrideResult apply(InstanceInfo instanceInfo,
                                      Lease<InstanceInfo> existingLease,
                                      boolean isReplication) {
        // 使用复合规则，顺序匹配，直到匹配成功
        for (int i = 0; i < this.rules.length; ++i) {
            StatusOverrideResult result = this.rules[i].apply(instanceInfo, existingLease, isReplication);
            if (result.matches()) {
                return result;
            }
        }
        // 使用默认规则
        return defaultRule.apply(instanceInfo, existingLease, isReplication);
    }

    @Override
    public String toString() {
        return this.compositeRuleName;
    }
}
