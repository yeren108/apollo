package com.ctrip.framework.apollo.configservice.util;

import com.ctrip.framework.apollo.common.entity.AppNamespace;
import com.ctrip.framework.apollo.configservice.service.AppNamespaceServiceWithCache;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@Component
public class NamespaceUtil {

  @Autowired
  private AppNamespaceServiceWithCache appNamespaceServiceWithCache;

  public String filterNamespaceName(String namespaceName) {
    if (namespaceName.toLowerCase().endsWith(".properties")) {
      int dotIndex = namespaceName.lastIndexOf(".");
      return namespaceName.substring(0, dotIndex);
    }

    return namespaceName;
  }

  public String normalizeNamespace(String appId, String namespaceName) {
    //找该appid下的私有namespace，找到返回
    AppNamespace appNamespace = appNamespaceServiceWithCache.findByAppIdAndNamespace(appId, namespaceName);
    if (appNamespace != null) {
      return appNamespace.getName();
    }

    //找公共的namespace，找到返回
    appNamespace = appNamespaceServiceWithCache.findPublicNamespaceByName(namespaceName);
    if (appNamespace != null) {
      return appNamespace.getName();
    }

    return namespaceName;
  }
}
