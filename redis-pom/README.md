# ${artifactId}-pom 

###install到本地仓库：
`mvn clean install -Dmaven.test.skip=true -Dfile.encoding=UTF-8 -Dmaven.javadoc.skip=true -U -T 1C -Pprod`
###deploy到私服：
`mvn clean deploy -Dmaven.test.skip=true -Dfile.encoding=UTF-8 -Dmaven.javadoc.skip=true -U -T 1C -Pprod` 

###单元测试testng+jmockit
1. eclipse安装testng插件  
2. eclipse安装code coverage插件，统计单元测试覆盖率