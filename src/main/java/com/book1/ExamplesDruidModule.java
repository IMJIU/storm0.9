/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.book1;

import java.util.Arrays;
import java.util.List;

import com.book1.twitter.TwitterSpritzerFirehoseFactory;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;

import io.druid.initialization.DruidModule;

/**
 */
public class ExamplesDruidModule implements DruidModule {
	@Override
	public List<? extends Module> getJacksonModules() {
		return Arrays.<Module>asList(new SimpleModule("ExamplesModule")
				.registerSubtypes(new NamedType(TwitterSpritzerFirehoseFactory.class, "twitzer")));
	}

	@Override
	public void configure(Binder binder) {

	}
}
