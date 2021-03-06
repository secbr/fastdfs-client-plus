
# 关于本项目

FastDFS Java Client Plus API may be copied only under the terms of the BSD license.

本项目分支来源于happyfish100官方fastdfs-client-java的1.29-SNAPSHOT版本，在此基础上进行逐步封装优化和二次开发，以便于更适合在项目中运用。

## 使用Maven依赖

RELEASE相关版本已发布到Maven中央仓库，在项目的pom.xml文件中直接配置如下配置，即可引入对应依赖：
```xml
<dependency>
    <groupId>com.github.secbr</groupId>
    <artifactId>fastdfs-client-plus</artifactId>
    <version>1.1.1-RELEASE</version>
</dependency>
```
项目中内置了slf4j-log4j12，如果在你的项目中已经引入了对应的日志框架，可进行如下的排除操作，防止jar包冲突。

```xml
<dependency>
    <groupId>com.github.secbr</groupId>
    <artifactId>fastdfs-client-plus</artifactId>
    <version>1.1.1-RELEASE</version>
    <exclusions>
        <exclusion>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-log4j12</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```
所需版本可根据需要进行替换对应的version，版本发布信息如下可访问该链接：https://github.com/secbr/fastdfs-client-plus/releases

如果需要通过本地打包或修改源代码之后构建可采用以下方式中一种：

### 使用ant从源码构建

```
ant clean package
```

### 使用maven从源码安装

```
mvn clean install
```

### 使用maven从jar文件安装
```
mvn install:install-file -DgroupId=com.github.secbr -DartifactId=fastdfs-client-plus -Dversion=${version} -Dpackaging=jar
 -Dfile=fastdfs-client-plus-${version}.jar
```

### 在maven项目pom.xml中添加依赖

通过上述构建之后，可以使用如下配置来进行引入：
```xml
<dependency>
    <groupId>com.github.secbr</groupId>
    <artifactId>fastdfs-client-plus</artifactId>
    <version>1.1.2-SNAPSHOT</version>
</dependency>
```

## .conf 配置文件、所在目录、加载优先顺序

    配置文件名fdfs_client.conf(或使用其它文件名xxx_yyy.conf)
    
    文件所在位置可以是项目classpath(或OS文件系统目录比如/opt/):
    /opt/fdfs_client.conf
    C:\Users\James\config\fdfs_client.conf
    
    优先按OS文件系统路径读取，没有找到才查找项目classpath，尤其针对linux环境下的相对路径比如：
    fdfs_client.conf
    config/fdfs_client.conf

```
connect_timeout = 2
network_timeout = 30
charset = UTF-8
http.tracker_http_port = 80
http.anti_steal_token = no
http.secret_key = FastDFS1234567890

tracker_server = 10.0.11.247:22122
tracker_server = 10.0.11.248:22122
tracker_server = 10.0.11.249:22122

connection_pool.enabled = true
connection_pool.max_count_per_entry = 500
connection_pool.max_idle_time = 3600
connection_pool.max_wait_time_in_ms = 1000
```

    注1：tracker_server指向您自己IP地址和端口，1-n个
    注2：除了tracker_server，其它配置项都是可选的


## .properties 配置文件、所在目录、加载优先顺序

    配置文件名 fastdfs-client.properties(或使用其它文件名 xxx-yyy.properties)
    
    文件所在位置可以是项目classpath(或OS文件系统目录比如/opt/):
    /opt/fastdfs-client.properties
    C:\Users\James\config\fastdfs-client.properties
    
    优先按OS文件系统路径读取，没有找到才查找项目classpath，尤其针对linux环境下的相对路径比如：
    fastdfs-client.properties
    config/fastdfs-client.properties

```
fastdfs.connect_timeout_in_seconds = 5
fastdfs.network_timeout_in_seconds = 30
fastdfs.charset = UTF-8
fastdfs.http_anti_steal_token = false
fastdfs.http_secret_key = FastDFS1234567890
fastdfs.http_tracker_http_port = 80

fastdfs.tracker_servers = 10.0.11.201:22122,10.0.11.202:22122,10.0.11.203:22122

fastdfs.connection_pool.enabled = true
fastdfs.connection_pool.max_count_per_entry = 500
fastdfs.connection_pool.max_idle_time = 3600
fastdfs.connection_pool.max_wait_time_in_ms = 1000
```

    注1：properties 配置文件中属性名跟 conf 配置文件不尽相同，并且统一加前缀"fastdfs."，便于整合到用户项目配置文件
    注2：fastdfs.tracker_servers 配置项不能重复属性名，多个 tracker_server 用逗号","隔开
    注3：除了fastdfs.tracker_servers，其它配置项都是可选的


## 加载配置示例

    加载原 conf 格式文件配置：
    ClientGlobal.init("fdfs_client.conf");
    ClientGlobal.init("config/fdfs_client.conf");
    ClientGlobal.init("/opt/fdfs_client.conf");
    ClientGlobal.init("C:\\Users\\James\\config\\fdfs_client.conf");

    加载 properties 格式文件配置：
    ClientGlobal.initByProperties("fastdfs-client.properties");
    ClientGlobal.initByProperties("config/fastdfs-client.properties");
    ClientGlobal.initByProperties("/opt/fastdfs-client.properties");
    ClientGlobal.initByProperties("C:\\Users\\James\\config\\fastdfs-client.properties");

    加载 Properties 对象配置：
    Properties props = new Properties();
    props.put(ClientGlobal.PROP_KEY_TRACKER_SERVERS, "10.0.11.101:22122,10.0.11.102:22122");
    ClientGlobal.initByProperties(props);

    加载 trackerServers 字符串配置：
    String trackerServers = "10.0.11.101:22122,10.0.11.102:22122";
    ClientGlobal.initByTrackers(trackerServers);


## 检查加载配置结果：
    
    System.out.println("ClientGlobal.configInfo(): " + ClientGlobal.configInfo());
```
ClientGlobal.configInfo(): {
  g_connect_timeout(ms) = 5000
  g_network_timeout(ms) = 30000
  g_charset = UTF-8
  g_anti_steal_token = false
  g_secret_key = FastDFS1234567890
  g_tracker_http_port = 80
  g_connection_pool_enabled = true
  g_connection_pool_max_count_per_entry = 500
  g_connection_pool_max_idle_time(ms) = 3600000
  g_connection_pool_max_wait_time_in_ms(ms) = 1000
  trackerServers = 10.0.11.101:22122,10.0.11.102:22122
}
```
