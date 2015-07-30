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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.sqoop.job.etl.Loader;
import org.apache.sqoop.job.etl.LoaderContext;

import com.feixian.sqoop.connector.hbase.configuration.LinkConfiguration;
import com.feixian.sqoop.connector.hbase.configuration.ToJobConfiguration;

public class HbaseLoader extends Loader<LinkConfiguration, ToJobConfiguration> {
	private static final Logger LOG = Logger.getLogger(HbaseLoader.class);
	private int rowWritten = 0;

	@Override
	public void load(LoaderContext context,
			LinkConfiguration linkConfiguration,
			ToJobConfiguration jobConfiguration) throws Exception {
		Object[] record = null;
		String tablename = jobConfiguration.toJobConfig.tablename;
		String columnsMap = jobConfiguration.toJobConfig.columnsMap;
		String rowkeyGenratedWay = jobConfiguration.toJobConfig.rowkeyGeneratedWay;
		String rowkeyParams = jobConfiguration.toJobConfig.rowkeyParams;

		String zookeeperlist = linkConfiguration.linkConfig.zookeeperlist;
		String zookeepernode = linkConfiguration.linkConfig.zookeepernode;

		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", zookeeperlist);
		conf.set("zookeeper.znode.parent", zookeepernode);

		String[] items = columnsMap.split(",");
		HashMap<Integer, byte[][]> index2familyAndColumn = new HashMap<Integer, byte[][]>();
		for (int i = 0; i < items.length; i++) {
			String[] splits = items[i].split(":");
			if (splits.length != 2) {
				throw new Exception(
						"HBASE LOADER's columnsMap error. columnsMap=family1:column1,...");
			}
			index2familyAndColumn.put(
					i,
					new byte[][] { Bytes.toBytes(splits[0]),
							Bytes.toBytes(splits[1]) });
		}

		HTable table = new HTable(conf, Bytes.toBytes(tablename));
		List<Put> puts = new ArrayList<Put>();
		while ((record = context.getDataReader().readArrayRecord()) != null) {
			if (items.length != record.length) {
				LOG.warn("HbaseLoader load record.length = " + record.length
						+ ", but columnsMap.length = " + items.length
						+ ". quit record.");
				continue;
			} else {
				byte[] rowkey = generateRowKey(record, rowkeyGenratedWay,
						rowkeyParams);
				Put put = new Put(rowkey);
				for (int i = 0; i < record.length; i++) {
					byte[][] familyAndColumn = index2familyAndColumn.get(i);
					if (record[i] instanceof Integer) {
						put.add(familyAndColumn[0], familyAndColumn[1],
								Bytes.toBytes((Integer) record[i]));
					} else {
						put.add(familyAndColumn[0], familyAndColumn[1],
								Bytes.toBytes(String.valueOf(record[i])));
					}
				}
				puts.add(put);
			}
		}
		table.put(puts);
		table.flushCommits();
		table.close();
		LOG.debug("upload docs successfully");
	}

	@Override
	public long getRowsWritten() {
		// TODO Auto-generated method stub
		return rowWritten;
	}

	private byte[] generateRowKey(Object[] records, String generatedWay,
			String rowkeyParams) throws Exception {
		if (generatedWay.equals("normal")) {
			String[] splits = rowkeyParams.split(",");
			byte[][] rowkeyparts = new byte[splits.length][];
			int byteslen = 0;
			for (int i = 0; i < splits.length; i++) {
				Object value = records[Integer.valueOf(splits[i])];
				if (value instanceof Integer) {
					rowkeyparts[i] = Bytes.toBytes((Integer) value);
				} else if (value instanceof Long) {
					rowkeyparts[i] = Bytes.toBytes((Long) value);
				} else if (value instanceof byte[]) {
					rowkeyparts[i] = (byte[]) value;
				} else {
					rowkeyparts[i] = Bytes.toBytes(String.valueOf(value));
				}
				byteslen += rowkeyparts[i].length;
			}
			int index = 0;
			byte[] rowkey = new byte[byteslen];
			for (byte[] rowkeypart : rowkeyparts) {
				System.arraycopy(rowkeypart, 0, rowkey, index,
						rowkeypart.length);
				index += rowkeypart.length;
			}
			return rowkey;
		} else {
			throw new Exception("unsupported RowKeyGeneratedWay");
		}
	}
}
