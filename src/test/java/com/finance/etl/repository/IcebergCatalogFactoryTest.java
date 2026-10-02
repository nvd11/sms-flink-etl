package com.finance.etl.repository;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.TableLoader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("IcebergCatalogFactory 湖仓基础设施工厂测试")
class IcebergCatalogFactoryTest {

    @Test
    @DisplayName("测试 Hadoop S3A 配置生成：验证关键参数与 s3:// 兼容前缀")
    void testCreateHadoopConf() {
        Configuration conf = IcebergCatalogFactory.createHadoopConf();
        assertNotNull(conf);
        assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", conf.get("fs.s3a.impl"));
        assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", conf.get("fs.s3.impl"), "必须包含 s3:// 兼容协议支持");
        assertEquals("true", conf.get("fs.s3a.path.style.access"));
    }

    @Test
    @DisplayName("测试 JDBC Catalog 属性集组装")
    void testCreateCatalogProperties() {
        Map<String, String> props = IcebergCatalogFactory.createCatalogProperties();
        assertNotNull(props);
        assertEquals("jdbc", props.get("type"));
        assertTrue(props.containsKey("uri"));
        assertTrue(props.containsKey("warehouse"));
    }

    @Test
    @DisplayName("测试 TableLoader 一键装配能力")
    void testCreateTableLoader() {
        TableLoader tableLoader = IcebergCatalogFactory.createTableLoader("finance_dev", "raw_sms_records");
        assertNotNull(tableLoader, "生成的 TableLoader 实例不能为 null");
    }
}
