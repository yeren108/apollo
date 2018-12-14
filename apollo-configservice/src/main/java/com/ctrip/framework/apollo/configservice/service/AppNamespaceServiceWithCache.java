package com.ctrip.framework.apollo.configservice.service;

import com.ctrip.framework.apollo.configservice.wrapper.CaseInsensitiveMapWrapper;
import com.ctrip.framework.apollo.core.utils.StringUtils;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.ctrip.framework.apollo.biz.config.BizConfig;
import com.ctrip.framework.apollo.biz.repository.AppNamespaceRepository;
import com.ctrip.framework.apollo.common.entity.AppNamespace;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Jason Song(song_s@ctrip.com)
 * 处理namespace缓存工具
 */
@Service
public class AppNamespaceServiceWithCache implements InitializingBean {
  private static final Logger logger = LoggerFactory.getLogger(AppNamespaceServiceWithCache.class);
  private static final Joiner STRING_JOINER = Joiner.on(ConfigConsts.CLUSTER_NAMESPACE_SEPARATOR)
      .skipNulls();
  @Autowired
  private AppNamespaceRepository appNamespaceRepository;

  @Autowired
  private BizConfig bizConfig;

  private int scanInterval;
  private TimeUnit scanIntervalTimeUnit;
  private int rebuildInterval;
  private TimeUnit rebuildIntervalTimeUnit;
  private ScheduledExecutorService scheduledExecutorService;
  private long maxIdScanned;

  //store namespaceName -> AppNamespace
  private CaseInsensitiveMapWrapper<AppNamespace> publicAppNamespaceCache;

  //store appId+namespaceName -> AppNamespace
  private CaseInsensitiveMapWrapper<AppNamespace> appNamespaceCache;

  //store id -> AppNamespace
  //存储（id，AppNamespace)键值对
  private Map<Long, AppNamespace> appNamespaceIdCache;

  public AppNamespaceServiceWithCache() {
    initialize();
  }

  private void initialize() {
    maxIdScanned = 0;
    publicAppNamespaceCache = new CaseInsensitiveMapWrapper<>(Maps.newConcurrentMap());
    appNamespaceCache = new CaseInsensitiveMapWrapper<>(Maps.newConcurrentMap());
    appNamespaceIdCache = Maps.newConcurrentMap();
    scheduledExecutorService = Executors.newScheduledThreadPool(1, ApolloThreadFactory
        .create("AppNamespaceServiceWithCache", true));
  }

  public AppNamespace findByAppIdAndNamespace(String appId, String namespaceName) {
    Preconditions.checkArgument(!StringUtils.isContainEmpty(appId, namespaceName), "appId and namespaceName must not be empty");
    return appNamespaceCache.get(STRING_JOINER.join(appId, namespaceName));
  }

  public List<AppNamespace> findByAppIdAndNamespaces(String appId, Set<String> namespaceNames) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(appId), "appId must not be null");
    if (namespaceNames == null || namespaceNames.isEmpty()) {
      return Collections.emptyList();
    }
    List<AppNamespace> result = Lists.newArrayList();
    for (String namespaceName : namespaceNames) {
      AppNamespace appNamespace = appNamespaceCache.get(STRING_JOINER.join(appId, namespaceName));
      if (appNamespace != null) {
        result.add(appNamespace);
      }
    }
    return result;
  }

  public AppNamespace findPublicNamespaceByName(String namespaceName) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(namespaceName), "namespaceName must not be empty");
    return publicAppNamespaceCache.get(namespaceName);
  }

  public List<AppNamespace> findPublicNamespacesByNames(Set<String> namespaceNames) {
    if (namespaceNames == null || namespaceNames.isEmpty()) {
      return Collections.emptyList();
    }

    List<AppNamespace> result = Lists.newArrayList();
    for (String namespaceName : namespaceNames) {
      AppNamespace appNamespace = publicAppNamespaceCache.get(namespaceName);
      if (appNamespace != null) {
        result.add(appNamespace);
      }
    }
    return result;
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    populateDataBaseInterval();
    scanNewAppNamespaces(); //block the startup process until load finished
    //60s一次更新缓存的定时任务
    scheduledExecutorService.scheduleAtFixedRate(() -> {
      Transaction transaction = Tracer.newTransaction("Apollo.AppNamespaceServiceWithCache",
          "rebuildCache");
      try {
        this.updateAndDeleteCache();
        transaction.setStatus(Transaction.SUCCESS);
      } catch (Throwable ex) {
        transaction.setStatus(ex);
        logger.error("Rebuild cache failed", ex);
      } finally {
        transaction.complete();
      }
    }, rebuildInterval, rebuildInterval, rebuildIntervalTimeUnit);
    //定时扫描namespace，1s执行一次scanNewAppNamespaces方法，而该方法每次批量取出500个namespce,直到取完所有的namespace
    scheduledExecutorService.scheduleWithFixedDelay(this::scanNewAppNamespaces, scanInterval,
        scanInterval, scanIntervalTimeUnit);
  }

  private void scanNewAppNamespaces() {
    Transaction transaction = Tracer.newTransaction("Apollo.AppNamespaceServiceWithCache",
        "scanNewAppNamespaces");
    try {
      this.loadNewAppNamespaces();
      transaction.setStatus(Transaction.SUCCESS);
    } catch (Throwable ex) {
      transaction.setStatus(ex);
      logger.error("Load new app namespaces failed", ex);
    } finally {
      transaction.complete();
    }
  }

  //for those new app namespaces
  private void loadNewAppNamespaces() {
    boolean hasMore = true;
    while (hasMore && !Thread.currentThread().isInterrupted()) {
      //current batch is 500
      //从数据库中批量取出500个namespace
      List<AppNamespace> appNamespaces = appNamespaceRepository
          .findFirst500ByIdGreaterThanOrderByIdAsc(maxIdScanned);
      if (CollectionUtils.isEmpty(appNamespaces)) {
        break;
      }
      //将从数据库中拿到的namespace合并到缓存
      mergeAppNamespaces(appNamespaces);
      int scanned = appNamespaces.size();
      maxIdScanned = appNamespaces.get(scanned - 1).getId();
      hasMore = scanned == 500;
      logger.info("Loaded {} new app namespaces with startId {}", scanned, maxIdScanned);
    }
  }

  //将从数据库中拿到的namespace合并到缓存
  private void mergeAppNamespaces(List<AppNamespace> appNamespaces) {
    for (AppNamespace appNamespace : appNamespaces) {
      //key=appid+namespaceName   value=appNamespace
      appNamespaceCache.put(assembleAppNamespaceKey(appNamespace), appNamespace);
      //key=id     value=appNamespace
      appNamespaceIdCache.put(appNamespace.getId(), appNamespace);
      //如果是公共的,存放到公共缓存中，key=namespaceName  value=appNamespace
      if (appNamespace.isPublic()) {
        publicAppNamespaceCache.put(appNamespace.getName(), appNamespace);
      }
    }
  }

  //for those updated or deleted app namespaces
  private void updateAndDeleteCache() {
    List<Long> ids = Lists.newArrayList(appNamespaceIdCache.keySet());
    if (CollectionUtils.isEmpty(ids)) {
      return;
    }
    //将namespace的id集合分为每500一个分区的的数组
    /*
      [a1,a2,...,a500]
      [a501,a5022,...,a1000]
      ...
    */
    List<List<Long>> partitionIds = Lists.partition(ids, 500);
    //相当于每次查找500个namespace
    for (List<Long> toRebuild : partitionIds) {
      Iterable<AppNamespace> appNamespaces = appNamespaceRepository.findAllById(toRebuild);

      if (appNamespaces == null) {
        continue;
      }

      //handle updated
      //更新 本批次 缓存中所有的namespace,并返回 本批次处理的namespace的id集合
      Set<Long> foundIds = handleUpdatedAppNamespaces(appNamespaces);

      //handle deleted
      //删除掉 本批次 中缓存中存在而数据库中不存在的namespace
      handleDeletedAppNamespaces(Sets.difference(Sets.newHashSet(toRebuild), foundIds));
    }
  }

  //for those updated app namespaces
  //处理所有更新的namspace
  private Set<Long> handleUpdatedAppNamespaces(Iterable<AppNamespace> appNamespaces) {
    Set<Long> foundIds = Sets.newHashSet();
    //遍历所有的namespace，
    for (AppNamespace appNamespace : appNamespaces) {
      foundIds.add(appNamespace.getId());
      //根据ID取出缓存中的AppNamespace
      AppNamespace thatInCache = appNamespaceIdCache.get(appNamespace.getId());
      //如果缓存的AppNamespace不为空，并且该appNamespace修改晚于缓存中的修改，将更新后的appNamespace缓存
      if (thatInCache != null && appNamespace.getDataChangeLastModifiedTime().after(thatInCache.getDataChangeLastModifiedTime())) {
        appNamespaceIdCache.put(appNamespace.getId(), appNamespace);
        String oldKey = assembleAppNamespaceKey(thatInCache);
        String newKey = assembleAppNamespaceKey(appNamespace);
        appNamespaceCache.put(newKey, appNamespace);

        //in case appId or namespaceName changes
        //如果appId或者namespaceName被更改了，也就是appId+namespaceName 变化了，就移除老的appId+namespaceName。
        if (!newKey.equals(oldKey)) {
          appNamespaceCache.remove(oldKey);
        }

        //如果namespace是公共的，将其缓存到publicAppNamespaceCache
        if (appNamespace.isPublic()) {
          publicAppNamespaceCache.put(appNamespace.getName(), appNamespace);

          //in case namespaceName changes
          //如果公共的namespace改变了，则去掉之前的缓存
          if (!appNamespace.getName().equals(thatInCache.getName()) && thatInCache.isPublic()) {
            publicAppNamespaceCache.remove(thatInCache.getName());
          }
        } else if (thatInCache.isPublic()) {
          //如果缓存中的namespace是公共的公共的namespace则删除这个缓存
          //just in case isPublic changes
          publicAppNamespaceCache.remove(thatInCache.getName());
        }
        logger.info("Found AppNamespace changes, old: {}, new: {}", thatInCache, appNamespace);
      }
    }
    //返回的是 本批次 从库中拿到的所有namespace的id集合。
    return foundIds;
  }

  //for those deleted app namespaces
  private void handleDeletedAppNamespaces(Set<Long> deletedIds) {
    if (CollectionUtils.isEmpty(deletedIds)) {
      return;
    }
    for (Long deletedId : deletedIds) {
      AppNamespace deleted = appNamespaceIdCache.remove(deletedId);
      if (deleted == null) {
        continue;
      }
      appNamespaceCache.remove(assembleAppNamespaceKey(deleted));
      if (deleted.isPublic()) {
        AppNamespace publicAppNamespace = publicAppNamespaceCache.get(deleted.getName());
        // in case there is some dirty data, e.g. public namespace deleted in some app and now created in another app
        if (publicAppNamespace == deleted) {
          publicAppNamespaceCache.remove(deleted.getName());
        }
      }
      logger.info("Found AppNamespace deleted, {}", deleted);
    }
  }

  //返回字符串  appid+namespaceName   eg:10001+application
  private String assembleAppNamespaceKey(AppNamespace appNamespace) {
    return STRING_JOINER.join(appNamespace.getAppId(), appNamespace.getName());
  }

  private void populateDataBaseInterval() {
    //1
    scanInterval = bizConfig.appNamespaceCacheScanInterval();
    //秒
    scanIntervalTimeUnit = bizConfig.appNamespaceCacheScanIntervalTimeUnit();
    //60
    rebuildInterval = bizConfig.appNamespaceCacheRebuildInterval();
    //秒
    rebuildIntervalTimeUnit = bizConfig.appNamespaceCacheRebuildIntervalTimeUnit();
  }

  //only for test use
  private void reset() throws Exception {
    scheduledExecutorService.shutdownNow();
    initialize();
    afterPropertiesSet();
  }
}
