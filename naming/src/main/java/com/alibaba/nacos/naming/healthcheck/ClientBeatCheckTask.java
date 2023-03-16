/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.naming.healthcheck;

import com.alibaba.nacos.common.utils.IPUtil;
import com.alibaba.nacos.common.http.Callback;
import com.alibaba.nacos.common.model.RestResult;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.sys.env.EnvUtil;
import com.alibaba.nacos.sys.utils.ApplicationUtils;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.core.Instance;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.healthcheck.events.InstanceHeartbeatTimeoutEvent;
import com.alibaba.nacos.naming.misc.GlobalConfig;
import com.alibaba.nacos.naming.misc.HttpClient;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.NamingProxy;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.push.PushService;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

/**
 * Check and update statues of ephemeral instances, remove them if they have been expired.
 *
 * @author nkorange
 */
public class ClientBeatCheckTask implements Runnable {
    
    private Service service;
    
    public ClientBeatCheckTask(Service service) {
        this.service = service;
    }
    
    @JsonIgnore
    public PushService getPushService() {
        return ApplicationUtils.getBean(PushService.class);
    }
    
    @JsonIgnore
    public DistroMapper getDistroMapper() {
        return ApplicationUtils.getBean(DistroMapper.class);
    }
    
    public GlobalConfig getGlobalConfig() {
        return ApplicationUtils.getBean(GlobalConfig.class);
    }
    
    public SwitchDomain getSwitchDomain() {
        return ApplicationUtils.getBean(SwitchDomain.class);
    }

    /**
     * 根据指定的服务构建任务key
     * @return
     */
    public String taskKey() {
        return KeyBuilder.buildServiceMetaKey(service.getNamespaceId(), service.getName());
    }

    /**
     * 心跳检查线程任务
     */
    @Override
    public void run() {
        try {
            //判断本机是否该负责指定服务的心跳检测任务
            if (!getDistroMapper().responsible(service.getName())) {
                //根据服务名选取特定的服务端节点做心跳检查任务
                return;
            }
            
            if (!getSwitchDomain().isHealthCheckEnabled()) {
                //是否启用服务的健康检查 默认启用返回true 取反 false 不进入if
                return;
            }
            //获取服务的所有临时实例
            List<Instance> instances = service.allIPs(true);
            
            // first set health status of instances:
            for (Instance instance : instances) {
                /**
                 * 遍历所有实例 判断系统当前时间与实例最后一次心跳时间对比 判断是否大于心跳超时时间  默认 15S
                 */
                if (System.currentTimeMillis() - instance.getLastBeat() > instance.getInstanceHeartBeatTimeOut()) {
                    if (!instance.isMarked()) {
                        if (instance.isHealthy()) {
                            //修改实例的健康状态
                            instance.setHealthy(false);
                            Loggers.EVT_LOG
                                    .info("{POS} {IP-DISABLED} valid: {}:{}@{}@{}, region: {}, msg: client timeout after {}, last beat: {}",
                                            instance.getIp(), instance.getPort(), instance.getClusterName(),
                                            service.getName(), UtilsAndCommons.LOCALHOST_SITE,
                                            instance.getInstanceHeartBeatTimeOut(), instance.getLastBeat());
                            //服务状态发生变化
                            getPushService().serviceChanged(service);
                            //发布事件-- 实例心跳检测超时
                            ApplicationUtils.publishEvent(new InstanceHeartbeatTimeoutEvent(this, instance));
                        }
                    }

                }
            }
            //是否启用实例自动过期删除 这里默认为true 取反则为false 代码不会返回 而是执行下面的删除服务的判断逻辑
            if (!getGlobalConfig().isExpireInstance()) {

                return;
            }
            //超过删除时间 30S
            // then remove obsolete instances:
            for (Instance instance : instances) {
                
                if (instance.isMarked()) {
                    continue;
                }
                //遍历所有服务 判断当前时间与实例的最后一次心跳检测的时间差是否大于 删除时间的阈值 默认30S
                if (System.currentTimeMillis() - instance.getLastBeat() > instance.getIpDeleteTimeout()) {
                    // delete instance
                    Loggers.SRV_LOG.info("[AUTO-DELETE-IP] service: {}, ip: {}", service.getName(),
                            JacksonUtils.toJson(instance));
                    //删除实例
                    deleteIp(instance);
                }
            }
            
        } catch (Exception e) {
            Loggers.SRV_LOG.warn("Exception while processing client beat time out.", e);
        }
        
    }
    //删除指定的实例
    private void deleteIp(Instance instance) {
        
        try {
            NamingProxy.Request request = NamingProxy.Request.newRequest();
            request.appendParam("ip", instance.getIp()).appendParam("port", String.valueOf(instance.getPort()))
                    .appendParam("ephemeral", "true").appendParam("clusterName", instance.getClusterName())
                    .appendParam("serviceName", service.getName()).appendParam("namespaceId", service.getNamespaceId());
            
            String url = "http://" + IPUtil.localHostIP() + IPUtil.IP_PORT_SPLITER + EnvUtil.getPort() + EnvUtil.getContextPath()
                    + UtilsAndCommons.NACOS_NAMING_CONTEXT + "/instance?" + request.toUrl();
            //http://127.0.0.1:8848/nacos/v1/ns/instance?ip=xxx&port=xxx
            // delete instance asynchronously:
            //异步删除服务实例
            HttpClient.asyncHttpDelete(url, null, null, new Callback<String>() {
                @Override
                public void onReceive(RestResult<String> result) {
                    if (!result.ok()) {
                        Loggers.SRV_LOG
                                .error("[IP-DEAD] failed to delete ip automatically, ip: {}, caused {}, resp code: {}",
                                        instance.toJson(), result.getMessage(), result.getCode());
                    }
                }
    
                @Override
                public void onError(Throwable throwable) {
                    Loggers.SRV_LOG
                            .error("[IP-DEAD] failed to delete ip automatically, ip: {}, error: ", instance.toJson(),
                                    throwable);
                }
    
                @Override
                public void onCancel() {
        
                }
            });
            
        } catch (Exception e) {
            Loggers.SRV_LOG
                    .error("[IP-DEAD] failed to delete ip automatically, ip: {}, error: {}", instance.toJson(), e);
        }
    }
}
