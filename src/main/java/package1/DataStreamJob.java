/*
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

package package1;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;


/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<PizzaOrder> orders =env.addSource(new PizzaOrderGenerator());

		//orders.print();


		DataStream<PizzaOrder> filteredOrders = orders
				// keep only those rides and both start and end in NYC
				.filter(new ShippedFilter());
		  filteredOrders.print();

		//Step-1 this is step 1 for icebergSinkUtil.java logic
		// Now we have to Convert PizzaOrder to RowData because iceberg support row data
		DataStream<RowData> rowDataStream = filteredOrders.map(order -> {
			GenericRowData row = new GenericRowData(6);
			row.setField(0, order.orderId);
			row.setField(1, order.placeTime.toEpochMilli());
			row.setField(2, order.addrLon);
			row.setField(3, order.addrLat);
			row.setField(4, order.pizzaType);
			row.setField(5, order.status);
			return row;
		});

		// Call the Iceberg sink utility to write to Iceberg
		IcebergSinkUtil.writeToIceberg(
				rowDataStream,
				"gs://cloudsql-functions-golang2/iceberg_warehouse", // Catalog path
				"cloudsql-functions-golang2",                        // Bucket name
				"pizza_orders"                             // Table name
		);


		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.fromSequence(1, 10);
		 *
		 * then, transform the resulting DataStream<Long> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.window()
		 * 	.process()
		 *
		 * and many more.
		 * Have a look at the programming guide:
		 *
		 * https://nightlies.apache.org/flink/flink-docs-stable/
		 *
		 */

		// Execute program, beginning computation.
		env.execute("Flink Java API Skeleton");
	}
	public static class ShippedFilter implements FilterFunction<PizzaOrder> {
		@Override
		public boolean filter(PizzaOrder Order) {
			return Order.status.equals("Shipped");
		}
	}
}
