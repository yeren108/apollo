package com.ctrip.framework.apollo.configservice.controller;

<<<<<<< HEAD
=======
import java.io.IOException;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

>>>>>>> e3c1cd89d7fedf9412b5d1f3ccd920c216106dfd
import com.ctrip.framework.apollo.biz.entity.Release;
import com.ctrip.framework.apollo.common.entity.AppNamespace;
import com.ctrip.framework.apollo.configservice.service.AppNamespaceServiceWithCache;
import com.ctrip.framework.apollo.configservice.service.config.ConfigService;
import com.ctrip.framework.apollo.configservice.util.InstanceConfigAuditUtil;
import com.ctrip.framework.apollo.configservice.util.NamespaceUtil;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.ctrip.framework.apollo.core.dto.ApolloConfig;
import com.ctrip.framework.apollo.core.dto.ApolloNotificationMessages;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@RestController
@RequestMapping("/configs")
public class ConfigController {
  private static final Splitter X_FORWARDED_FOR_SPLITTER = Splitter.on(",").omitEmptyStrings()
      .trimResults();
  private final ConfigService configService;
  private final AppNamespaceServiceWithCache appNamespaceService;
  private final NamespaceUtil namespaceUtil;
  private final InstanceConfigAuditUtil instanceConfigAuditUtil;
  private final Gson gson;

  private static final Type configurationTypeReference = new TypeToken<Map<String, String>>() {
      }.getType();
<<<<<<< HEAD

  public ConfigController(
      final ConfigService configService,
      final AppNamespaceServiceWithCache appNamespaceService,
      final NamespaceUtil namespaceUtil,
      final InstanceConfigAuditUtil instanceConfigAuditUtil,
      final Gson gson) {
    this.configService = configService;
    this.appNamespaceService = appNamespaceService;
    this.namespaceUtil = namespaceUtil;
    this.instanceConfigAuditUtil = instanceConfigAuditUtil;
    this.gson = gson;
  }

  @GetMapping(value = "/{appId}/{clusterName}/{namespace:.+}")
=======
  private static final Logger logger = LoggerFactory.getLogger(ConfigController.class);
  /**
   *
   *  http://ip:port/configs/appid/cluster/namespace?dataCenter=dataCenterStr&ip=ipStr
   *  &message=messageStr&releaseKey=releaseKeyStr
   *
   *  eg:
   *  http://1.1.1.1:8080/configs/10001/default/FX.grayscaleIP.properties?dataCenter=gl&ip=127.0.0.1
   *  &message={"details":{"10001+default+FX.grayscaleIP":1159}}&releaseKey=20181101155641-af002bf0f95b6782
   *
   *  这个接口是补偿机制接口，每5分钟，客户端主动从服务端获取一次配置。
   *
   */
  @RequestMapping(value = "/{appId}/{clusterName}/{namespace:.+}", method = RequestMethod.GET)
>>>>>>> e3c1cd89d7fedf9412b5d1f3ccd920c216106dfd
  public ApolloConfig queryConfig(@PathVariable String appId, @PathVariable String clusterName,
                                  @PathVariable String namespace,
                                  @RequestParam(value = "dataCenter", required = false) String dataCenter,
                                  @RequestParam(value = "releaseKey", defaultValue = "-1") String clientSideReleaseKey,
                                  @RequestParam(value = "ip", required = false) String clientIp,
                                  @RequestParam(value = "messages", required = false) String messagesAsString,
                                  HttpServletRequest request, HttpServletResponse response) throws IOException {
    SimpleDateFormat myFmt=new SimpleDateFormat("yyyy年MM月dd日 HH时mm分ss秒");
    logger.info("---------------------------------------------------------------------queryConfig--------->>"+String.valueOf(myFmt.format(new Date())));
    String originalNamespace = namespace;
    //namespace去掉.properties后缀
    namespace = namespaceUtil.filterNamespaceName(namespace);
    //fix the character case issue, such as FX.apollo <-> fx.apollo
    namespace = namespaceUtil.normalizeNamespace(appId, namespace);

    //如果clientIp不存在，则拿到请求的ip
    if (Strings.isNullOrEmpty(clientIp)) {
      clientIp = tryToGetClientIp(request);
    }

    //eg: messagesAsString = “detail”:{"10001+default+FX.grayscaleIP":1159}
    //key="appId+cluster+namespace"  value=通知id
    ApolloNotificationMessages clientMessages = transformMessages(messagesAsString);

    List<Release> releases = Lists.newLinkedList();

    String appClusterNameLoaded = clusterName;
    //appid不等于ApolloNoAppIdPlaceHolder时
    if (!ConfigConsts.NO_APPID_PLACEHOLDER.equalsIgnoreCase(appId)) {
      //当前release版本
      Release currentAppRelease = configService.loadConfig(appId, clientIp, appId, clusterName, namespace, dataCenter, clientMessages);

      if (currentAppRelease != null) {
        //当前release版本不为空时，拿出来
        releases.add(currentAppRelease);
        //we have cluster search process, so the cluster name might be overridden
        //
        appClusterNameLoaded = currentAppRelease.getClusterName();
      }
    }

    //if namespace does not belong to this appId, should check if there is a public configuration
    //如果namespace不属于该appId,需要检查其是否为公共配置
    if (!namespaceBelongsToAppId(appId, namespace)) {
      Release publicRelease = this.findPublicConfig(appId, clientIp, clusterName, namespace,
          dataCenter, clientMessages);
      if (!Objects.isNull(publicRelease)) {
        releases.add(publicRelease);
      }
    }

    //私有和公共配置都找不到
    if (releases.isEmpty()) {
      response.sendError(HttpServletResponse.SC_NOT_FOUND,String.format("Could not load configurations with appId: %s, clusterName: %s, namespace: %s",appId, clusterName, originalNamespace));
      Tracer.logEvent("Apollo.Config.NotFound",assembleKey(appId, clusterName, originalNamespace, dataCenter));
      return null;
    }
    //审计
    auditReleases(appId, clusterName, dataCenter, clientIp, releases);

    String mergedReleaseKey = releases.stream().map(Release::getReleaseKey).collect(Collectors.joining(ConfigConsts.CLUSTER_NAMESPACE_SEPARATOR));

    //客户端版本和服务器版本一致，返回304
    if (mergedReleaseKey.equals(clientSideReleaseKey)) {
      // Client side configuration is the same with server side, return 304
      response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
      Tracer.logEvent("Apollo.Config.NotModified",assembleKey(appId, appClusterNameLoaded, originalNamespace, dataCenter));
      return null;
    }
    //不一致时，返回当前版本的配置
    ApolloConfig apolloConfig = new ApolloConfig(appId, appClusterNameLoaded, originalNamespace,mergedReleaseKey);
    apolloConfig.setConfigurations(mergeReleaseConfigurations(releases));

    Tracer.logEvent("Apollo.Config.Found", assembleKey(appId, appClusterNameLoaded,originalNamespace, dataCenter));
    return apolloConfig;
  }

  private boolean namespaceBelongsToAppId(String appId, String namespaceName) {
    //Every app has an 'application' namespace
    //每一个app都有一个叫application的namespace，所以返回true
    if (Objects.equals(ConfigConsts.NAMESPACE_APPLICATION, namespaceName)) {
      return true;
    }

    //if no appId is present, then no other namespace belongs to it
    //如果没有appid，namespace就不属于任何appid
    if (ConfigConsts.NO_APPID_PLACEHOLDER.equalsIgnoreCase(appId)) {
      return false;
    }
    //数据库中查询appid下是否含有该namespace
    AppNamespace appNamespace = appNamespaceService.findByAppIdAndNamespace(appId, namespaceName);

    return appNamespace != null;
  }

  /**
   * @param clientAppId the application which uses public config
   * @param namespace   the namespace
   * @param dataCenter  the datacenter
   */
  private Release findPublicConfig(String clientAppId, String clientIp, String clusterName,
                                   String namespace, String dataCenter, ApolloNotificationMessages clientMessages) {
    //根据namespace名称找到共有的namespace
    AppNamespace appNamespace = appNamespaceService.findPublicNamespaceByName(namespace);

    //check whether the namespace's appId equals to current one
    if (Objects.isNull(appNamespace) || Objects.equals(clientAppId, appNamespace.getAppId())) {
      return null;
    }

    String publicConfigAppId = appNamespace.getAppId();

    return configService.loadConfig(clientAppId, clientIp, publicConfigAppId, clusterName, namespace, dataCenter,
        clientMessages);
  }

  /**
   * Merge configurations of releases.
   * Release in lower index override those in higher index
   */
  Map<String, String> mergeReleaseConfigurations(List<Release> releases) {
    Map<String, String> result = Maps.newHashMap();
    for (Release release : Lists.reverse(releases)) {
      result.putAll(gson.fromJson(release.getConfigurations(), configurationTypeReference));
    }
    return result;
  }

  private String assembleKey(String appId, String cluster, String namespace, String dataCenter) {
    List<String> keyParts = Lists.newArrayList(appId, cluster, namespace);
    if (!Strings.isNullOrEmpty(dataCenter)) {
      keyParts.add(dataCenter);
    }
    return keyParts.stream().collect(Collectors.joining(ConfigConsts.CLUSTER_NAMESPACE_SEPARATOR));
  }

  //审计
  private void auditReleases(String appId, String cluster, String dataCenter, String clientIp,List<Release> releases) {
    if (Strings.isNullOrEmpty(clientIp)) {
      //no need to audit instance config when there is no ip
      return;
    }
    for (Release release : releases) {
      instanceConfigAuditUtil.audit(appId, cluster, dataCenter, clientIp, release.getAppId(),release.getClusterName(),release.getNamespaceName(), release.getReleaseKey());
    }
  }

  //获取客户端IP
  private String tryToGetClientIp(HttpServletRequest request) {
    String forwardedFor = request.getHeader("X-FORWARDED-FOR");
    if (!Strings.isNullOrEmpty(forwardedFor)) {
      return X_FORWARDED_FOR_SPLITTER.splitToList(forwardedFor).get(0);
    }
    return request.getRemoteAddr();
  }

  //eg: messagesAsString = “detail”:{"10001+default+FX.grayscaleIP":1159}
  ApolloNotificationMessages transformMessages(String messagesAsString) {
    ApolloNotificationMessages notificationMessages = null;
    if (!Strings.isNullOrEmpty(messagesAsString)) {
      try {
        notificationMessages = gson.fromJson(messagesAsString, ApolloNotificationMessages.class);
      } catch (Throwable ex) {
        Tracer.logError(ex);
      }
    }

    return notificationMessages;
  }
}
