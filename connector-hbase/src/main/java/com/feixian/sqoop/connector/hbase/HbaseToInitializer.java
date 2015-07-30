/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.feixian.sqoop.connector.hbase;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.sqoop.job.etl.Initializer;
import org.apache.sqoop.job.etl.InitializerContext;
import org.apache.sqoop.utils.ClassUtils;

import com.feixian.sqoop.connector.hbase.configuration.LinkConfiguration;
import com.feixian.sqoop.connector.hbase.configuration.ToJobConfiguration;

public class HbaseToInitializer extends
		Initializer<LinkConfiguration, ToJobConfiguration> {

	private static final Logger LOG = Logger
			.getLogger(HbaseToInitializer.class);

	@Override
	public void initialize(InitializerContext context,
			LinkConfiguration linkConfiguration,
			ToJobConfiguration jobConfiguration) {
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum",
				linkConfiguration.linkConfig.zookeeperlist);
		conf.set("zookeeper.znode.parent",
				linkConfiguration.linkConfig.zookeepernode);
		try {
			HBaseAdmin hbaseadmin = new HBaseAdmin(conf);
			byte[] tablebytes = Bytes
					.toBytes(jobConfiguration.toJobConfig.tablename);
			if (hbaseadmin.tableExists(tablebytes) == false) {
				if (Boolean.valueOf(jobConfiguration.toJobConfig.createTable)) {
					@SuppressWarnings("deprecation")
					HTableDescriptor descriptor = new HTableDescriptor(
							tablebytes);
					String[] cols = jobConfiguration.toJobConfig.columnsMap
							.split(",");
					HashSet<String> familys = new HashSet<String>();
					for (String col : cols) {
						String family = col.split(":")[0];
						familys.add(family);
					}
					for (String family : familys) {
						descriptor.addFamily(new HColumnDescriptor(Bytes
								.toBytes(family)));
					}
					hbaseadmin.createTable(descriptor);

				} else {
					LOG.error("hbase table was not exists... tablename = "
							+ jobConfiguration.toJobConfig.tablename);
				}
			}
			hbaseadmin.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.error("HBASE initialize error... when check hbase table...", e);
		}
		LOG.info("Running hbase Connector initializer. This does nothing except log this message.");
	}

	@Override
	public Set<String> getJars(InitializerContext context,
			LinkConfiguration linkConfiguration,
			ToJobConfiguration toJobConfiguration) {
		Set<String> jars = super.getJars(context, linkConfiguration,
				toJobConfiguration);
		// Jars for Kafka, Scala and Yammer (required by Kafka)
		jars.add(ClassUtils
				.jarForClass("org.apache.hadoop.hbase.HColumnDescriptor"));
		jars.add(ClassUtils.jarForClass("org.apache.hadoop.hbase.util.Bytes"));
		jars.add(ClassUtils.jarForClass("org.apache.hadoop.conf.Configuration"));
		jars.add(ClassUtils
				.jarForClass("org.apache.hadoop.hbase.protobuf.generated.MasterProtos"));
		jars.add(ClassUtils
				.jarForClass("org.I0Itec.zkclient.ContentWatcher"));
		jars.add(ClassUtils
				.jarForClass("org.cloudera.htrace.Trace"));
		return jars;
	}

}
