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

package com.alibaba.nacos.naming.consistency;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.naming.consistency.ephemeral.EphemeralConsistencyService;
import com.alibaba.nacos.naming.consistency.persistent.PersistentConsistencyServiceDelegateImpl;
import com.alibaba.nacos.naming.pojo.Record;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;

import java.util.Optional;

/**
 * Consistency delegate.
 *
 * @author nkorange
 * @since 1.0.0
 */
@DependsOn("ProtocolManager")
@Service("consistencyDelegate")
public class DelegateConsistencyServiceImpl implements ConsistencyService {
    
    private final PersistentConsistencyServiceDelegateImpl persistentConsistencyService;
    
    private final EphemeralConsistencyService ephemeralConsistencyService;
    
    public DelegateConsistencyServiceImpl(PersistentConsistencyServiceDelegateImpl persistentConsistencyService,
            EphemeralConsistencyService ephemeralConsistencyService) {
        this.persistentConsistencyService = persistentConsistencyService;
        this.ephemeralConsistencyService = ephemeralConsistencyService;
    }
    
    @Override
    public void put(String key, Record value) throws NacosException {
        //根据key 获取一致性处理的Service
        /**
         * ephemeralConsistencyService 临时的
         * persistentConsistencyService 持久的
         */
        ConsistencyService consistencyService = mapConsistencyService(key);
        consistencyService.put(key, value);
    }
    
    @Override
    public void remove(String key) throws NacosException {
        mapConsistencyService(key).remove(key);
    }

    /**
     * 获取
     * @param key key of data
     * @return
     * @throws NacosException
     */
    @Override
    public Datum get(String key) throws NacosException {
        //ephemeralConsistencyService 临时实例 DistroConsistencyServiceImpl
        //persistentConsistencyService 持久实例 PersistentConsistencyServiceDelegateImpl
        ConsistencyService consistencyService = mapConsistencyService(key);
        return consistencyService.get(key);
    }
    
    @Override
    public void listen(String key, RecordListener listener) throws NacosException {
        
        // this special key is listened by both:
        if (KeyBuilder.SERVICE_META_KEY_PREFIX.equals(key)) {
            persistentConsistencyService.listen(key, listener);
            ephemeralConsistencyService.listen(key, listener);
            return;
        }
        
        mapConsistencyService(key).listen(key, listener);
    }
    
    @Override
    public void unListen(String key, RecordListener listener) throws NacosException {
        mapConsistencyService(key).unListen(key, listener);
    }
    
    @Override
    public boolean isAvailable() {
        return ephemeralConsistencyService.isAvailable() && persistentConsistencyService.isAvailable();
    }
    
    @Override
    public Optional<String> getErrorMsg() {
        String errorMsg;
        if (ephemeralConsistencyService.getErrorMsg().isPresent()
                && persistentConsistencyService.getErrorMsg().isPresent()) {
            errorMsg = "'" + ephemeralConsistencyService.getErrorMsg().get() + "' in Distro protocol and '"
                    + persistentConsistencyService.getErrorMsg().get() + "' in jRaft protocol";
        } else if (ephemeralConsistencyService.getErrorMsg().isPresent()
                && !persistentConsistencyService.getErrorMsg().isPresent()) {
            errorMsg = ephemeralConsistencyService.getErrorMsg().get();
        } else if (!ephemeralConsistencyService.getErrorMsg().isPresent()
                && persistentConsistencyService.getErrorMsg().isPresent()) {
            errorMsg = persistentConsistencyService.getErrorMsg().get();
        } else {
            errorMsg = null;
        }
        return Optional.ofNullable(errorMsg);
    }
    
    private ConsistencyService mapConsistencyService(String key) {
        //判断操作的是临时实例 还是持久实例 AP一致性处理  还是CP一致性处理
        return KeyBuilder.matchEphemeralKey(key) ? ephemeralConsistencyService : persistentConsistencyService;
    }
}
