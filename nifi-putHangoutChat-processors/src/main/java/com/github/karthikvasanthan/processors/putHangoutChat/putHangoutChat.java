/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.karthikvasanthan.processors.putHangoutChat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.*;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.*;

@Tags({"hangouts","custom","chat","google"})
@CapabilityDescription("Sends a message to google hangouts group")
@SeeAlso({})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class putHangoutChat extends AbstractProcessor {

    public static final PropertyDescriptor SPACE_ID = new PropertyDescriptor
            .Builder().name("Space ID")
            .description("The space to which message is to be sent")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor JSON_CREDS = new PropertyDescriptor
            .Builder().name("Json Credentials")
            .description("Credentials for the service account that will write to google sheets")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CARD_HEADER = new PropertyDescriptor
            .Builder().name("Card Header")
            .description("The header for the card")

            .expressionLanguageSupported(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Message sent successfully")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Message could not be sent")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private static final String HANGOUTS_CHAT_API_SCOPE = "https://www.googleapis.com/auth/chat.bot";
    private static final String RESPONSE_URL_TEMPLATE =  "https://chat.googleapis.com/v1/__SPACE_ID__/messages";
    private HttpRequestFactory requestFactory;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(SPACE_ID);
        descriptors.add(JSON_CREDS);
        descriptors.add(CARD_HEADER);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException, GeneralSecurityException {
        GoogleCredential credential;
        PropertyValue serviceAccountCredentialsJsonProperty = context.getProperty(JSON_CREDS);
        credential = GoogleCredential.fromStream(new ByteArrayInputStream(serviceAccountCredentialsJsonProperty.getValue().getBytes(StandardCharsets.UTF_8)))
                .createScoped(Collections.singleton(HANGOUTS_CHAT_API_SCOPE));
        HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        requestFactory = httpTransport.createRequestFactory(credential);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        final String spaceId = context.getProperty(SPACE_ID).evaluateAttributeExpressions(flowFile).getValue();
        String cardHeader = context.getProperty(CARD_HEADER).isSet() ? context.getProperty(CARD_HEADER).evaluateAttributeExpressions(flowFile).getValue() : null;

        String URI =
                RESPONSE_URL_TEMPLATE.replaceFirst(
                        "__SPACE_ID__","spaces/" + spaceId);
        GenericUrl url = new GenericUrl(URI);
        String cardString = "";
        CardResponseBuilder cardResponseBuilder = new CardResponseBuilder();

        final ObjectMapper mapper = new ObjectMapper();
        try {
            LinkedHashMap<Object, Object> msgMap = mapper.readValue(session.read(flowFile), new TypeReference<LinkedHashMap<Object, Object>>() {
            });
            msgMap.values().removeIf(Objects::isNull);

            if (cardHeader == null) {
                cardString = cardResponseBuilder
                        .keyValue(msgMap).build();
            }
            else {
                cardString = cardResponseBuilder
                        .header(cardHeader,"","")
                        .keyValue(msgMap).build();
            }
        }
        catch (IOException e) {
            getLogger().error("IO Exception when reading JSON item: " + e.getMessage());
            session.transfer(flowFile,REL_FAILURE);
        }

        try {
            HttpContent content =
                    new ByteArrayContent("application/json", cardString.getBytes("UTF-8"));
            HttpRequest request = requestFactory.buildPostRequest(url, content);
            request.execute();
            session.transfer(flowFile,REL_SUCCESS);
        } catch (IOException e) {
            getLogger().error("IO Exception encountered: " + e.getMessage());
            session.transfer(flowFile,REL_FAILURE);
        }

        session.commit();

    }
}
