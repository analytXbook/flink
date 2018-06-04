/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.environment;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.clusterframework.messages.GetClusterStatusResponse;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;

import org.apache.flink.streaming.api.graph.StreamGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.List;

/**
 * The LocalStreamEnvironment is a StreamExecutionEnvironment that runs the program locally,
 * multi-threaded, in the JVM where the environment is instantiated. It spawns an embedded
 * Flink cluster in the background and executes the program on that cluster.
 *
 * <p>When this environment is instantiated, it uses a default parallelism of {@code 1}. The default
 * parallelism can be set via {@link #setParallelism(int)}.
 *
 * <p>Local environments can also be instantiated through {@link StreamExecutionEnvironment#createLocalEnvironment()}
 * and {@link StreamExecutionEnvironment#createLocalEnvironment(int)}. The former version will pick a
 * default parallelism equal to the number of hardware contexts in the local machine.
 */
@Public
public class LocalStreamEnvironment extends StreamExecutionEnvironment {

	private static final Logger LOG = LoggerFactory.getLogger(LocalStreamEnvironment.class);

	/** The configuration to use for the local cluster. */
	private final Configuration conf;

	LocalFlinkMiniCluster exec;

	/**
	 * Creates a new local stream environment that uses the default configuration.
	 */
	public LocalStreamEnvironment() {
		this(null);
	}

	/**
	 * Creates a new local stream environment that configures its local executor with the given configuration.
	 *
	 * @param config The configuration used to configure the local executor.
	 */
	public LocalStreamEnvironment(Configuration config) {
		if (!ExecutionEnvironment.areExplicitEnvironmentsAllowed()) {
			throw new InvalidProgramException(
					"The LocalStreamEnvironment cannot be used when submitting a program through a client, " +
							"or running in a TestEnvironment context.");
		}

		this.conf = config == null ? new Configuration() : config;
	}

	/**
	 * Executes the JobGraph of the on a mini cluster of CLusterUtil with a user
	 * specified name.
	 *
	 * @param jobName
	 *            name of the job
	 * @return The result of the job execution, containing elapsed time and accumulators.
	 */
	@Override
	public JobSubmissionResult executeDetached(String jobName) throws Exception {
		return execute(jobName);
	}

	/**
	 * Executes the JobGraph of the on a mini cluster of CLusterUtil with a user
	 * specified name.
	 *
	 * @param jobName
	 *            name of the job
	 * @return The result of the job execution, containing elapsed time and accumulators.
	 */
	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		return execute(jobName, false).getJobExecutionResult();
	}

	/**
	 * Executes the JobGraph of the on a mini cluster of CLusterUtil with a user
	 * specified name.
	 *
	 * @param jobName
	 *            name of the job
	 * @return The result of the job execution, containing elapsed time and accumulators.
	 */
	public JobSubmissionResult execute(String jobName, boolean detached) throws Exception {
		// transform the streaming program into a JobGraph
		StreamGraph streamGraph = getStreamGraph();
		streamGraph.setJobName(jobName);

		JobGraph jobGraph = streamGraph.getJobGraph();

		if (LOG.isInfoEnabled()) {
			LOG.info("Running job on local embedded Flink mini cluster");
		}

		LocalFlinkMiniCluster exec = getCluster();

		if (detached) {
			return exec.submitJobDetached(jobGraph);
		} else {
			try {
				return exec.submitJobAndWait(jobGraph, getConfig().isSysoutLoggingEnabled());
			} finally {
				transformations.clear();
				exec = null;
				exec.stop();
			}
		}
	}

	private Configuration getLocalConfig() {
		Configuration configuration = new Configuration();
		configuration.addAll(this.conf);

		configuration.setLong(TaskManagerOptions.MANAGED_MEMORY_SIZE, -1L);
		configuration.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, getMaxParallelism());
		return configuration;
	}

	private LocalFlinkMiniCluster getCluster() {
		if (exec == null) {
			exec = new LocalFlinkMiniCluster(getLocalConfig(), true);
			exec.start();
		}
		return exec;
	}

	@Override
	public ClusterClient getClusterClient() throws Exception{
		class LocalFlinkMiniClusterClient extends ClusterClient {
			LocalFlinkMiniCluster cluster;
			public LocalFlinkMiniClusterClient(Configuration configuration) throws Exception {
				super(configuration);
				this.cluster = getCluster();
			}

			@Override
			public void waitForClusterToBeReady() { }

			@Override
			public String getWebInterfaceURL() {
				return null;
			}

			@Override
			public GetClusterStatusResponse getClusterStatus() {
				return null;
			}

			@Override
			protected List<String> getNewMessages() {
				return null;
			}

			@Override
			public String getClusterIdentifier() {
				return null;
			}

			@Override
			protected void finalizeCluster() { }

			@Override
			public int getMaxSlots() { return 0; }

			@Override
			public boolean hasUserJarsInClassPath(List<URL> userJarFiles) {
				return false;
			}

			@Override
			protected JobSubmissionResult submitJob(JobGraph jobGraph, ClassLoader classLoader) throws ProgramInvocationException {
				try {
					return cluster.submitJobDetached(jobGraph);
				} catch (JobExecutionException e) {
					throw new ProgramInvocationException(e.getMessage());
				}
			}
		}

		return new LocalFlinkMiniClusterClient(getLocalConfig());
	}
}
