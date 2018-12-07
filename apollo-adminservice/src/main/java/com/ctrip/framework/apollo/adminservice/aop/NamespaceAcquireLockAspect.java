package com.ctrip.framework.apollo.adminservice.aop;


import com.ctrip.framework.apollo.biz.config.BizConfig;
import com.ctrip.framework.apollo.biz.entity.Item;
import com.ctrip.framework.apollo.biz.entity.Namespace;
import com.ctrip.framework.apollo.biz.entity.NamespaceLock;
import com.ctrip.framework.apollo.biz.service.ItemService;
import com.ctrip.framework.apollo.biz.service.NamespaceLockService;
import com.ctrip.framework.apollo.biz.service.NamespaceService;
import com.ctrip.framework.apollo.common.dto.ItemChangeSets;
import com.ctrip.framework.apollo.common.dto.ItemDTO;
import com.ctrip.framework.apollo.common.exception.BadRequestException;
import com.ctrip.framework.apollo.common.exception.ServiceException;

import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;


/**
 * 一个namespace在一次发布中只能允许一个人修改配置
 * 通过数据库lock表来实现
 */
@Aspect
@Component
public class NamespaceAcquireLockAspect {
  private static final Logger logger = LoggerFactory.getLogger(NamespaceAcquireLockAspect.class);


  @Autowired
  private NamespaceLockService namespaceLockService;
  @Autowired
  private NamespaceService namespaceService;
  @Autowired
  private ItemService itemService;
  @Autowired
  private BizConfig bizConfig;


  //create item
  @Before("@annotation(PreAcquireNamespaceLock) && args(appId, clusterName, namespaceName, item, ..)")
  public void requireLockAdvice(String appId, String clusterName, String namespaceName,
                                ItemDTO item) {
    acquireLock(appId, clusterName, namespaceName, item.getDataChangeLastModifiedBy());
  }

  //update item
  @Before("@annotation(PreAcquireNamespaceLock) && args(appId, clusterName, namespaceName, itemId, item, ..)")
  public void requireLockAdvice(String appId, String clusterName, String namespaceName, long itemId,
                                ItemDTO item) {
    acquireLock(appId, clusterName, namespaceName, item.getDataChangeLastModifiedBy());
  }

  //update by change set
  @Before("@annotation(PreAcquireNamespaceLock) && args(appId, clusterName, namespaceName, changeSet, ..)")
  public void requireLockAdvice(String appId, String clusterName, String namespaceName,
                                ItemChangeSets changeSet) {
    acquireLock(appId, clusterName, namespaceName, changeSet.getDataChangeLastModifiedBy());
  }

  //delete item
  @Before("@annotation(PreAcquireNamespaceLock) && args(itemId, operator, ..)")
  public void requireLockAdvice(long itemId, String operator) {
    Item item = itemService.findOne(itemId);
    if (item == null){
      throw new BadRequestException("item not exist.");
    }
    acquireLock(item.getNamespaceId(), operator);
  }

  void acquireLock(String appId, String clusterName, String namespaceName,String currentUser) {
    //开关配置没有打开，则不需要获取锁
    if (bizConfig.isNamespaceLockSwitchOff()) {
      return;
    }
    //从数据库中拿到当前appId下的当前cluster的当前namespace
    Namespace namespace = namespaceService.findOne(appId, clusterName, namespaceName);
    //获取当前用户的锁
    acquireLock(namespace, currentUser);
  }

  void acquireLock(long namespaceId, String currentUser) {
    if (bizConfig.isNamespaceLockSwitchOff()) {
      return;
    }

    Namespace namespace = namespaceService.findOne(namespaceId);

    acquireLock(namespace, currentUser);

  }

  private void acquireLock(Namespace namespace, String currentUser) {
    if (namespace == null) {
      throw new BadRequestException("namespace not exist.");
    }

    long namespaceId = namespace.getId();
    //从ApolloConfigDB中查找表NamespaceLock，看当前namespaceId在其中是否存在记录
    NamespaceLock namespaceLock = namespaceLockService.findLock(namespaceId);
    //如果不存在则获取锁
    if (namespaceLock == null) {
      try {
        //获取锁，即在表NamespaceLock新增一条该用户的记录
        tryLock(namespaceId, currentUser);
        //lock success
      } catch (DataIntegrityViolationException e) {
        //lock fail
        namespaceLock = namespaceLockService.findLock(namespaceId);
        checkLock(namespace, namespaceLock, currentUser);
      } catch (Exception e) {
        logger.error("try lock error", e);
        throw e;
      }
    } else {
      //如果存在则检查当前用户是否拥有锁
      //check lock owner is current user
      checkLock(namespace, namespaceLock, currentUser);
    }
  }

  private void tryLock(long namespaceId, String user) {
    NamespaceLock lock = new NamespaceLock();
    lock.setNamespaceId(namespaceId);
    lock.setDataChangeCreatedBy(user);
    lock.setDataChangeLastModifiedBy(user);
    namespaceLockService.tryLock(lock);
  }

  private void checkLock(Namespace namespace, NamespaceLock namespaceLock,
                         String currentUser) {
    if (namespaceLock == null) {
      throw new ServiceException(
          String.format("Check lock for %s failed, please retry.", namespace.getNamespaceName()));
    }

    String lockOwner = namespaceLock.getDataChangeCreatedBy();
    if (!lockOwner.equals(currentUser)) {
      throw new BadRequestException(
          "namespace:" + namespace.getNamespaceName() + " is modified by " + lockOwner);
    }
  }


}
