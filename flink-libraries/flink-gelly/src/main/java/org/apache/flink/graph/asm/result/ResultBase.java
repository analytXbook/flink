/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.graph.asm.result;

/**
 * Base class for algorithm results.
 */
public abstract class ResultBase {

	/**
	 * {@link Object#toString()} must be overridden to write POJO values in the
	 * same form as {@link org.apache.flink.api.java.tuple.Tuple}. Values are
	 * comma-separated and enclosed in parenthesis, e.g. "(f0,f1)".
	 *
	 * @return tuple representation string
	 */
	@Override
	public abstract String toString();
}
