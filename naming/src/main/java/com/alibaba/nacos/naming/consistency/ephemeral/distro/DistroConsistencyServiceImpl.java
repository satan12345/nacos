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

package com.alibaba.nacos.naming.consistency.ephemeral.distro;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.common.utils.Objects;
import com.alibaba.nacos.naming.cluster.ServerStatus;
import com.alibaba.nacos.naming.cluster.transport.Serializer;
import com.alibaba.nacos.consistency.DataOperation;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.RecordListener;
import com.alibaba.nacos.naming.consistency.ephemeral.EphemeralConsistencyService;
import com.alibaba.nacos.naming.consistency.ephemeral.distro.combined.DistroHttpCombinedKey;
import com.alibaba.nacos.core.distributed.distro.DistroProtocol;
import com.alibaba.nacos.core.distributed.distro.component.DistroDataProcessor;
import com.alibaba.nacos.core.distributed.distro.entity.DistroData;
import com.alibaba.nacos.core.distributed.distro.entity.DistroKey;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.core.Instances;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.GlobalConfig;
import com.alibaba.nacos.naming.misc.GlobalExecutor;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.pojo.Record;
import org.apache.commons.lang3.StringUtils;
import org.javatuples.Pair;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A consistency protocol algorithm called <b>Distro</b>
 *
 * <p>Use a distro algorithm to divide data into many blocks. Each Nacos server node takes responsibility for exactly
 * one block of data. Each block of data is generated, removed and synchronized by its responsible server. So every
 * Nacos server only handles writings for a subset of the total service data.
 *
 * <p>At mean time every Nacos server receives data sync of other Nacos server, so every Nacos server will eventually
 * have a complete set of data.
 *
 * @author nkorange
 * @since 1.0.0
 */

/**
 * 临时实例一致性处理的Service
 * AP架构 一致性处理
 */
@DependsOn("ProtocolManager")
@org.springframework.stereotype.Service("distroConsistencyService")
public class DistroConsistencyServiceImpl implements EphemeralConsistencyService, DistroDataProcessor {
    
    private static final String ON_RECEIVE_CHECKSUMS_PROCESSING_TAG = "1";
    
    private final DistroMapper distroMapper;
    /**
     * 临时节点的数据 数据存储
     */
    private final DataStore dataStore;
    
    private final Serializer serializer;
    
    private final SwitchDomain switchDomain;
    
    private final GlobalConfig globalConfig;
    /**
     * 分布式数据一致性的一种协议
     */
    private final DistroProtocol distroProtocol;
    
    private volatile Notifier notifier = new Notifier();
    
    private Map<String, ConcurrentLinkedQueue<RecordListener>> listeners = new ConcurrentHashMap<>();
    
    private Map<String, String> syncChecksumTasks = new ConcurrentHashMap<>(16);
    
    public DistroConsistencyServiceImpl(DistroMapper distroMapper, DataStore dataStore, Serializer serializer,
            SwitchDomain switchDomain, GlobalConfig globalConfig, DistroProtocol distroProtocol) {
        this.distroMapper = distroMapper;
        this.dataStore = dataStore;
        this.serializer = serializer;
        this.switchDomain = switchDomain;
        this.globalConfig = globalConfig;
        this.distroProtocol = distroProtocol;
    }
    
    @PostConstruct
    public void init() {
        //线程池执行任务
        GlobalExecutor.submitDistroNotifyTask(notifier);
    }

    /**
     * 保存
     * @param key   key of data, this key should be globally unique
     * @param value value of data
     * @throws NacosException
     */
    @Override
    public void put(String key, Record value) throws NacosException {
        onPut(key, value);//本机内存中保存一份数据
        //数据同步 延迟1S
        distroProtocol.sync(new DistroKey(key, KeyBuilder.INSTANCE_LIST_KEY_PREFIX), DataOperation.CHANGE,
                globalConfig.getTaskDispatchPeriod() / 2);
    }
    
    @Override
    public void remove(String key) throws NacosException {
        onRemove(key);
        listeners.remove(key);
    }

    /**
     * 根据key 获取数据
     * @param key key of data
     * @return
     * @throws NacosException
     */
    @Override
    public Datum get(String key) throws NacosException {
        return dataStore.get(key);
    }
    
    /**
     * Put a new record.
     *
     * @param key   key of record
     * @param value record
     */
    public void onPut(String key, Record value) {
        
        if (KeyBuilder.matchEphemeralInstanceListKey(key)) {
            //如果是临时节点
            //封装对象
            Datum<Instances> datum = new Datum<>();
            datum.value = (Instances) value;
            datum.key = key;
            datum.timestamp.incrementAndGet();
            //保存key的与实例数据的对应关系
            dataStore.put(key, datum);
        }

        /**
         * 如果已经包含了这个服务的key 则返回true 取反为false  不返回 执行下面的触发数据变化的通知
         * 如果没有包含 则返回false 取反为true 进入if 直接返回 不执行下面的任务
         */
        if (!listeners.containsKey(key)) {
            return;
        }
        //添加任务
        notifier.addTask(key, DataOperation.CHANGE);
    }
    
    /**
     * Remove a record.
     *
     * @param key key of record
     */
    public void onRemove(String key) {
        
        dataStore.remove(key);
        
        if (!listeners.containsKey(key)) {
            return;
        }
        
        notifier.addTask(key, DataOperation.DELETE);
    }
    
    /**
     * Check sum when receive checksums request.
     *
     * @param checksumMap map of checksum
     * @param server      source server request checksum
     */
    public void onReceiveChecksums(Map<String, String> checksumMap, String server) {
        
        if (syncChecksumTasks.putIfAbsent(server, ON_RECEIVE_CHECKSUMS_PROCESSING_TAG) != null) {
            // Already in process of this server:
            Loggers.DISTRO.warn("sync checksum task already in process with {}", server);
            return;
        }
        
        try {
            
            List<String> toUpdateKeys = new ArrayList<>();
            List<String> toRemoveKeys = new ArrayList<>();
            for (Map.Entry<String, String> entry : checksumMap.entrySet()) {
                if (distroMapper.responsible(KeyBuilder.getServiceName(entry.getKey()))) {
                    // this key should not be sent from remote server:
                    Loggers.DISTRO.error("receive responsible key timestamp of " + entry.getKey() + " from " + server);
                    // abort the procedure:
                    return;
                }
                
                if (!dataStore.contains(entry.getKey()) || dataStore.get(entry.getKey()).value == null || !dataStore
                        .get(entry.getKey()).value.getChecksum().equals(entry.getValue())) {
                    toUpdateKeys.add(entry.getKey());
                }
            }
            
            for (String key : dataStore.keys()) {
                
                if (!server.equals(distroMapper.mapSrv(KeyBuilder.getServiceName(key)))) {
                    continue;
                }
                
                if (!checksumMap.containsKey(key)) {
                    toRemoveKeys.add(key);
                }
            }
            
            Loggers.DISTRO
                    .info("to remove keys: {}, to update keys: {}, source: {}", toRemoveKeys, toUpdateKeys, server);
            
            for (String key : toRemoveKeys) {
                onRemove(key);
            }
            
            if (toUpdateKeys.isEmpty()) {
                return;
            }
            
            try {
                DistroHttpCombinedKey distroKey = new DistroHttpCombinedKey(KeyBuilder.INSTANCE_LIST_KEY_PREFIX,
                        server);
                distroKey.getActualResourceTypes().addAll(toUpdateKeys);
                DistroData remoteData = distroProtocol.queryFromRemote(distroKey);
                if (null != remoteData) {
                    processData(remoteData.getContent());
                }
            } catch (Exception e) {
                Loggers.DISTRO.error("get data from " + server + " failed!", e);
            }
        } finally {
            // Remove this 'in process' flag:
            syncChecksumTasks.remove(server);
        }
    }
    
    private boolean processData(byte[] data) throws Exception {
        if (data.length > 0) {
            Map<String, Datum<Instances>> datumMap = serializer.deserializeMap(data, Instances.class);
            
            for (Map.Entry<String, Datum<Instances>> entry : datumMap.entrySet()) {
                dataStore.put(entry.getKey(), entry.getValue());
                
                if (!listeners.containsKey(entry.getKey())) {
                    // pretty sure the service not exist:
                    if (switchDomain.isDefaultInstanceEphemeral()) {
                        // create empty service
                        Loggers.DISTRO.info("creating service {}", entry.getKey());
                        Service service = new Service();
                        String serviceName = KeyBuilder.getServiceName(entry.getKey());
                        String namespaceId = KeyBuilder.getNamespace(entry.getKey());
                        service.setName(serviceName);
                        service.setNamespaceId(namespaceId);
                        service.setGroupName(Constants.DEFAULT_GROUP);
                        // now validate the service. if failed, exception will be thrown
                        service.setLastModifiedMillis(System.currentTimeMillis());
                        service.recalculateChecksum();
                        
                        // The Listener corresponding to the key value must not be empty
                        RecordListener listener = listeners.get(KeyBuilder.SERVICE_META_KEY_PREFIX).peek();
                        if (Objects.isNull(listener)) {
                            return false;
                        }
                        listener.onChange(KeyBuilder.buildServiceMetaKey(namespaceId, serviceName), service);
                    }
                }
            }
            
            for (Map.Entry<String, Datum<Instances>> entry : datumMap.entrySet()) {
                
                if (!listeners.containsKey(entry.getKey())) {
                    // Should not happen:
                    Loggers.DISTRO.warn("listener of {} not found.", entry.getKey());
                    continue;
                }
                
                try {
                    for (RecordListener listener : listeners.get(entry.getKey())) {
                        listener.onChange(entry.getKey(), entry.getValue().value);
                    }
                } catch (Exception e) {
                    Loggers.DISTRO.error("[NACOS-DISTRO] error while execute listener of key: {}", entry.getKey(), e);
                    continue;
                }
                
                // Update data store if listener executed successfully:
                dataStore.put(entry.getKey(), entry.getValue());
            }
        }
        return true;
    }
    
    @Override
    public boolean processData(DistroData distroData) {
        DistroHttpData distroHttpData = (DistroHttpData) distroData;
        Datum<Instances> datum = (Datum<Instances>) distroHttpData.getDeserializedContent();
        onPut(datum.key, datum.value);
        return true;
    }
    
    @Override
    public String processType() {
        return KeyBuilder.INSTANCE_LIST_KEY_PREFIX;
    }
    
    @Override
    public boolean processVerifyData(DistroData distroData) {
        DistroHttpData distroHttpData = (DistroHttpData) distroData;
        String sourceServer = distroData.getDistroKey().getResourceKey();
        Map<String, String> verifyData = (Map<String, String>) distroHttpData.getDeserializedContent();
        onReceiveChecksums(verifyData, sourceServer);
        return true;
    }
    
    @Override
    public boolean processSnapshot(DistroData distroData) {
        try {
            return processData(distroData.getContent());
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 保存关系
     * @param key      key of data
     * @param listener callback of data change
     * @throws NacosException
     */
    @Override
    public void listen(String key, RecordListener listener) throws NacosException {
        //根据key获取服务
        ConcurrentLinkedQueue<RecordListener> recordListeners = listeners.get(key);
        if (recordListeners == null) {
            recordListeners = new ConcurrentLinkedQueue<>();
            ConcurrentLinkedQueue<RecordListener> recordListenersExist;
            if ((recordListenersExist = listeners.putIfAbsent(key, recordListeners)) != null) {
                recordListeners = recordListenersExist;
            }
        }
        //DCL检查 不存在 添加
        if (!recordListeners.contains(listener)) {
            synchronized (recordListeners) {
                if (!recordListeners.contains(listener)) {
                    recordListeners.add(listener);
                }
            }
        }
    }
    
    @Override
    public void unListen(String key, RecordListener listener) throws NacosException {
        ConcurrentLinkedQueue<RecordListener> recordListeners = listeners.get(key);
        if (recordListeners == null) {
            return;
        }
        synchronized (recordListeners) {
            for (RecordListener recordListener : recordListeners) {
                if (recordListener.equals(listener)) {
                    recordListeners.remove(listener);
                    break;
                }
            }
        }
    }
    
    @Override
    public boolean isAvailable() {
        return isInitialized() || ServerStatus.UP.name().equals(switchDomain.getOverriddenServerStatus());
    }
    
    @Override
    public Optional<String> getErrorMsg() {
        String errorMsg;
        if (!isInitialized() && !ServerStatus.UP.name().equals(switchDomain.getOverriddenServerStatus())) {
            errorMsg = "Distro protocol is not initialized";
        } else {
            errorMsg = null;
        }
        return Optional.ofNullable(errorMsg);
    }
    
    public boolean isInitialized() {
        return distroProtocol.isInitialized() || !globalConfig.isDataWarmup();
    }

    /**
     * 通知的组件
     */
    public class Notifier implements Runnable {

        /**
         * 这个map 用于记录当前在队列中的key
         * 会在入队的时候放入map
         * 出队的时候从map中移除
         */
        private ConcurrentHashMap<String, String> services = new ConcurrentHashMap<>(10 * 1024);

        private BlockingQueue<Pair<String, DataOperation>> tasks = new ArrayBlockingQueue<>(1024 * 1024);
        
        /**
         * Add new notify task to queue.
         * 添加任务
         * @param datumKey data key
         * @param action   action for data
         */
        public void addTask(String datumKey, DataOperation action) {

            //做去重判断
            if (services.containsKey(datumKey) && action == DataOperation.CHANGE) {
                return;
            }
            if (action == DataOperation.CHANGE) {
                services.put(datumKey, StringUtils.EMPTY);
            }
            //参数封装对象 然后入队
            Pair<String, DataOperation> with = Pair.with(datumKey, action);
            tasks.offer(with);
        }
        
        public int getTaskSize() {
            return tasks.size();
        }
        //执行执行任务
        @Override
        public void run() {
            Loggers.DISTRO.info("distro notifier started");
            
            for (; ; ) {
                try {
                    //从队列中获取数据
                    Pair<String, DataOperation> pair = tasks.take();
                    //处理数据
                    handle(pair);
                } catch (Throwable e) {
                    Loggers.DISTRO.error("[NACOS-DISTRO] Error while handling notifying task", e);
                }
            }
        }

        /**
         * 任务处理
         * @param pair
         */
        private void handle(Pair<String, DataOperation> pair) {
            try {

                String datumKey = pair.getValue0();
                DataOperation action = pair.getValue1();
                //出队后从service中移除key
                services.remove(datumKey);
                
                int count = 0;
                // 这里根据key获取对应的Service 通过key -->Service 找到对应的
                ConcurrentLinkedQueue<RecordListener> recordListeners = listeners.get(datumKey);
                if (recordListeners == null) {
                    Loggers.DISTRO.info("[DISTRO-WARN] RecordListener not found, key: {}", datumKey);
                    return;
                }
                
                for (RecordListener listener : recordListeners) {
                    
                    count++;
                    System.out.println("action = " + action);
                    try {
                        if (action == DataOperation.CHANGE) {
                            Datum datum = dataStore.get(datumKey);
                            if (datum != null) {
                                listener.onChange(datumKey, datum.value);
                            } else {
                                Loggers.DISTRO.info("[DISTRO-WARN] data not found, key: {}", datumKey);
                            }
                            continue;
                        }
                        
                        if (action == DataOperation.DELETE) {
                            listener.onDelete(datumKey);
                            continue;
                        }
                    } catch (Throwable e) {
                        Loggers.DISTRO.error("[NACOS-DISTRO] error while notifying listener of key: {}", datumKey, e);
                    }
                }
                
                if (Loggers.DISTRO.isDebugEnabled()) {
                    Loggers.DISTRO
                            .debug("[NACOS-DISTRO] datum change notified, key: {}, listener count: {}, action: {}",
                                    datumKey, count, action.name());
                }
            } catch (Throwable e) {
                Loggers.DISTRO.error("[NACOS-DISTRO] Error while handling notifying task", e);
            }
        }
    }
}
