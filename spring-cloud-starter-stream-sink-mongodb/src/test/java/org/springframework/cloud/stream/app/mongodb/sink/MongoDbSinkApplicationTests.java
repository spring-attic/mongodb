/*
 * Copyright 2017-2018 the original author or authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.integration.mongodb.store.MessageDocument;
import org.springframework.integration.mongodb.support.MongoDbMessageBytesConverter;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.integration.support.MutableMessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author Artem Bilan
 * @author Chris Schaefer
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
		properties = {
				"spring.data.mongodb.port=0"
		})
@DirtiesContext
public abstract class MongoDbSinkApplicationTests {

	@Autowired
	protected MongoTemplate mongoTemplate;

	@Autowired
	protected Sink sink;

	@Autowired
	protected MongoDbSinkProperties mongoDbSinkProperties;

	@TestPropertySource(properties = "mongodb.collection=testing")
	static public class CollectionNameTests extends MongoDbSinkApplicationTests {

		@Test
		public void test() {
			Map<String, String> data1 = Collections.singletonMap("foo", "bar");

			Map<String, String> data2 = new HashMap<>();
			data2.put("firstName", "Foo");
			data2.put("lastName", "Bar");

			this.sink.input().send(new GenericMessage<>(data1));
			this.sink.input().send(new GenericMessage<>(data2));
			this.sink.input().send(new GenericMessage<>("{\"my_data\": \"THE DATA\"}"));

			List<Document> result =
					this.mongoTemplate.findAll(Document.class, mongoDbSinkProperties.getCollection());

			assertEquals(3, result.size());

			Document dbObject = result.get(0);
			assertNotNull(dbObject.get("_id"));
			assertEquals(dbObject.get("foo"), "bar");
			assertNotNull(dbObject.get("_class"));

			dbObject = result.get(1);
			assertEquals(dbObject.get("firstName"), "Foo");
			assertEquals(dbObject.get("lastName"), "Bar");

			dbObject = result.get(2);
			assertNull(dbObject.get("_class"));
			assertEquals(dbObject.get("my_data"), "THE DATA");
		}



	}

	@TestPropertySource(properties = "mongodb.collection-expression=headers.collection")
	static public class CollectionExpressionStoreMessageTests extends MongoDbSinkApplicationTests {

		@Test
		@SuppressWarnings("rawtypes")
		public void test() {
			Message<String> mutableMessage = MutableMessageBuilder.withPayload("foo")
					.setHeader("test", "1")
					.build();

			this.sink.input()
					.send(MessageBuilder.withPayload(new MessageDocument(mutableMessage))
							.setHeader("collection", "testing2")
							.build());

			List<MessageDocument> result = this.mongoTemplate.findAll(MessageDocument.class, "testing2");

			assertEquals(1, result.size());
			Message<?> message = result.get(0).getMessage();
			assertEquals(mutableMessage, message);
		}

	}

	@SpringBootApplication
	@EntityScan(basePackageClasses = MessageDocument.class)
	public static class MongoSinkApplication {

		@Bean
		public MongoCustomConversions customConversions() {
			return new MongoCustomConversions(Collections.singletonList(new MongoDbMessageBytesConverter()));
		}

	}

}
