/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.mongodb.sink;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.expression.Expression;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.mongodb.outbound.MongoDbStoringMessageHandler;
import org.springframework.messaging.MessageHandler;

/**
 * A starter configuration for MongoDB Sink applications.
 * Produces {@link MongoDbStoringMessageHandler} which ingests
 * incoming data into MongoDB Collection.
 *
 * @author Artem Bilan
 *
 */
@EnableBinding(Sink.class)
@EnableConfigurationProperties(MongoDbSinkProperties.class)
public class MongodbSinkConfiguration {

	@Autowired
	private MongoDbSinkProperties properties;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Bean
	@ServiceActivator(inputChannel = Sink.INPUT)
	public MessageHandler mongoDbSinkMessageHandler() {
		MongoDbStoringMessageHandler mongoDbMessageHandler = new MongoDbStoringMessageHandler(this.mongoTemplate);
		Expression collectionExpression = this.properties.getCollectionExpression();
		if (collectionExpression == null) {
			collectionExpression = new LiteralExpression(this.properties.getCollection());
		}
		mongoDbMessageHandler.setCollectionNameExpression(collectionExpression);
		return mongoDbMessageHandler;
	}

}
