/*
 * Copyright 2016-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.mongodb.source;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.app.trigger.TriggerConfiguration;
import org.springframework.cloud.stream.app.trigger.TriggerPropertiesMaxMessagesDefaultUnlimited;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.expression.Expression;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlowBuilder;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.mongodb.inbound.MongoDbMessageSource;
import org.springframework.messaging.MessageChannel;

/**
 * A starter configuration for MongoDB Source applications.
 * Produces {@link MongoDbMessageSource} which polls collection
 * with the query after startup according to the polling properties.
 *
 * @author Adam Zwickey
 * @author Artem Bilan
 *
 */
@EnableBinding(Source.class)
@EnableConfigurationProperties({ MongodbSourceProperties.class, TriggerPropertiesMaxMessagesDefaultUnlimited.class })
@Import(TriggerConfiguration.class)
public class MongodbSourceConfiguration {

	@Autowired
	private MongodbSourceProperties config;

	@Autowired
	@Qualifier(Source.OUTPUT)
	private MessageChannel output;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Bean
	public IntegrationFlow startFlow() throws Exception {
		IntegrationFlowBuilder flow = IntegrationFlows.from(mongoSource());
		if (config.isSplit()) {
			flow.split();
		}
		flow.channel(output);
		return flow.get();
	}

	/**
	 * The inheritors can consider to override this method for their purpose or just adjust options
	 * for the returned instance
	 * @return a {@link MongoDbMessageSource} instance
	 */
	protected MongoDbMessageSource mongoSource() {
		Expression queryExpression = (this.config.getQueryExpression() != null
				? this.config.getQueryExpression()
				: new LiteralExpression(this.config.getQuery()));
		MongoDbMessageSource mongoDbMessageSource = new MongoDbMessageSource(this.mongoTemplate, queryExpression);
		mongoDbMessageSource.setCollectionNameExpression(new LiteralExpression(this.config.getCollection()));
		mongoDbMessageSource.setEntityClass(String.class);
		return mongoDbMessageSource;
	}

}
