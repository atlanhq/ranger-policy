/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.tagsync.source.atlas;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.tagsync.source.atlasrest.RangerAtlasEntity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AtlanCustomResourceMapper extends AtlasResourceMapper {

    private static final Log LOG = LogFactory.getLog(AtlanCustomResourceMapper.class);


    public static final String ENTITY_TYPE_ATLAN_DB     = "Database";
    public static final String ENTITY_TYPE_ATLAN_SCHEMA = "Schema";
    public static final String ENTITY_TYPE_ATLAN_TABLE  = "Table";
    public static final String ENTITY_TYPE_ATLAN_COLUMN = "Column";

    public static final String RANGER_TYPE_ATLAN_DB     = "database";
    public static final String RANGER_TYPE_ATLAN_SCHEMA = "schema";
    public static final String RANGER_TYPE_ATLAN_TABLE  = "table";
    public static final String RANGER_TYPE_ATLAN_COLUMN = "column";
    
    public static final String RESOURCE_TYPE_CATEGORY                 = "type-category";
    public static final String RESOURCE_TYPE_NAME                     = "type";
    public static final String RESOURCE_ENTITY_TYPE                   = "entity-type";
    public static final String RESOURCE_ENTITY_CLASSIFICATION         = "entity-classification";
    public static final String RESOURCE_CLASSIFICATION                = "classification";
    public static final String RESOURCE_ENTITY_ID                     = "entity";
    public static final String RESOURCE_ENTITY_LABEL                  = "entity-label";
    public static final String RESOURCE_ENTITY_BUSINESS_METADATA      = "entity-business-metadata";
    public static final String RESOURCE_ENTITY_OWNER                  = "owner";

    public static final String ATLAN_DELIMITER = "/";
    public static final String ASTERISK = "*";

    public static final String[] SUPPORTED_ENTITY_TYPES = { ENTITY_TYPE_ATLAN_DB, ENTITY_TYPE_ATLAN_TABLE, ENTITY_TYPE_ATLAN_COLUMN };

    public AtlanCustomResourceMapper(){
        super("atlas", SUPPORTED_ENTITY_TYPES);
    }

    public AtlanCustomResourceMapper(String componentName, String[] supportedEntityTypes) {
        super(componentName, supportedEntityTypes);
    }


    @Override
    public RangerServiceResource buildResource(RangerAtlasEntity entity) throws Exception {

        String qualifiedName = (String)entity.getAttributes().get(AtlasResourceMapper.ENTITY_ATTRIBUTE_QUALIFIED_NAME);

        if (StringUtils.isEmpty(qualifiedName)) {
            throw new Exception("attribute '" +  ENTITY_ATTRIBUTE_QUALIFIED_NAME + "' not found in entity");
        }

        String resourceStr = getResourceNameFromQualifiedName(qualifiedName);

        if (StringUtils.isEmpty(resourceStr)) {
            throwExceptionWithMessage("resource not found in attribute '" +  ENTITY_ATTRIBUTE_QUALIFIED_NAME + "': " + qualifiedName);
        }

        // TODO - In Atlan is there any concept of cluster ?
        String clusterName = getClusterNameFromQualifiedName(qualifiedName);

        if (StringUtils.isEmpty(clusterName)) {
            throwExceptionWithMessage("cluster-name not found in attribute '" +  ENTITY_ATTRIBUTE_QUALIFIED_NAME + "': " + qualifiedName);
        }

        String   entityType  = entity.getTypeName();
        String   entityId = (String)entity.getAttributes().get(AtlasResourceMapper.ENTITY_ATTRIBUTE_QUALIFIED_NAME);

        List<EntityNotificationWrapper.RangerAtlasClassification>  entityClassifications =  entity.getTags();

        List<String> classificationNamesList = new ArrayList<>();

        if (entityClassifications != null) {
            for (EntityNotificationWrapper.RangerAtlasClassification classification : entityClassifications) {
                classificationNamesList.add(classification.getName());
            }
        }

        String   entityGuid  = entity.getGuid();
        String   serviceName = getRangerServiceName(clusterName);

        // DB -     default/snowflake/nitya-test-connection-api/TEST_DB
        // Schema - default/snowflake/nitya-test-connection-api/SUDH_TEST_DBT/PUBLIC
        // Table -  default/snowflake/nitya-test-connection-api/TEST_LINEAGE/PUBLIC/BENCHMARK316
        // Column   default/snowflake/nitya-test-connection-api/DEMO_DB/PUBLIC/COVID_STATE_LEVEL_STATS_AGG/TOT_ACTIVE



        String[] resources   = qualifiedName.split(ATLAN_DELIMITER);
        String   tenantId    = resources.length > 0 ? resources[0] : null;
        String   vendor      = resources.length > 1 ? resources[1] : null;
        String   connnection = resources.length > 2 ? resources[2] : null;
        String   dbName      = resources.length > 3 ? resources[3] : null;
        String   schemaName  = resources.length > 4 ? resources[4] : null;
        String   tblName     = resources.length > 5 ? resources[5] : null;
        String   colName     = resources.length > 6 ? resources[6] : null;

        Map<String, RangerPolicy.RangerPolicyResource> elements = new HashMap<String, RangerPolicy.RangerPolicyResource>();

        LOG.info(" entity type =>" + entityType + " tenantId = " + tenantId + " vendor = "+ vendor +"  connection= " + connnection +" dbName = "+ dbName + "schemaName="+ schemaName +"tblName= "+ tblName+ " entityClassifications "+ entityClassifications);

        //TODO - DO we want to specific entity types ?
        if (StringUtils.equals(entityType, ENTITY_TYPE_ATLAN_DB) || StringUtils.equals(entityType, ENTITY_TYPE_ATLAN_SCHEMA) ||
                StringUtils.equals(entityType, ENTITY_TYPE_ATLAN_TABLE) || StringUtils.equals(entityType, ENTITY_TYPE_ATLAN_TABLE) || StringUtils.equals(entityType, ENTITY_TYPE_ATLAN_COLUMN)){

            if  (StringUtils.isNotEmpty(entityType) && StringUtils.isNotEmpty(qualifiedName) && !classificationNamesList.isEmpty())
            {
                elements.put(RESOURCE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource(ASTERISK));
                elements.put(RESOURCE_ENTITY_CLASSIFICATION, new RangerPolicy.RangerPolicyResource(classificationNamesList, null,null));
  //              elements.put(RESOURCE_CLASSIFICATION, new RangerPolicy.RangerPolicyResource(classificationNamesList, null,null));
                elements.put(RESOURCE_ENTITY_ID, new RangerPolicy.RangerPolicyResource("*"));
            }

        } else {
            throwExceptionWithMessage("unrecognized entity-type: " + entityType);
        }


        if(elements.isEmpty()) {
            throwExceptionWithMessage("skipping since classifications are not present "+ entityClassifications + " qualifiedName for entity-type '" + entityType + "': " + qualifiedName);
        }

        RangerServiceResource ret = new RangerServiceResource(entityGuid, serviceName, elements);

        return ret;

    }

    protected  String getResourceNameFromQualifiedName(String qualifiedName) {
        if(StringUtils.isNotBlank(qualifiedName)) {
            int idx = qualifiedName.lastIndexOf(ATLAN_DELIMITER);

            if(idx != -1) {
                return qualifiedName.substring(0, idx);
            } else {
                return qualifiedName;
            }
        }

        return null;
    }


    protected  String getClusterNameFromQualifiedName(String qualifiedName) {

        if(StringUtils.isNotBlank(qualifiedName)) {
            String[] clusterArr = qualifiedName.split(ATLAN_DELIMITER);
            if (clusterArr.length > 0) {
                return clusterArr[0];
            }
        }
        return null;
    }

    public String getRangerServiceName(String clusterName) {
        String ret = getCustomRangerServiceName(clusterName);

        if (StringUtils.isBlank(ret)) {
            ret = clusterName + TAGSYNC_DEFAULT_CLUSTERNAME_AND_COMPONENTNAME_SEPARATOR + componentName;
        }
        return ret;
    }


    //ranger.tagsync.atlas.<component>.instance.<%default%>.ranger.service

    protected String getCustomRangerServiceName(String atlasInstanceName) {
        if(properties != null) {
            String propName = TAGSYNC_SERVICENAME_MAPPER_PROP_PREFIX + componentName
                    + TAGSYNC_ATLAS_CLUSTER_IDENTIFIER + atlasInstanceName
                    + TAGSYNC_SERVICENAME_MAPPER_PROP_SUFFIX;

            return properties.getProperty(propName);
        } else {
            return null;
        }
    }
}
