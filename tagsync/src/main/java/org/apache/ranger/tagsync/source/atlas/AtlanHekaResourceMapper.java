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

import org.apache.atlas.model.instance.AtlasClassification;
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

public class AtlanHekaResourceMapper extends AtlasResourceMapper {

    private static final Log LOG = LogFactory.getLog(AtlanCustomResourceMapper.class);


    public static final String HEKA_SERVICE_NAME        = "heka";
    public static final String ENTITY_TYPE_ATLAN_DB     = "Database";
    public static final String ENTITY_TYPE_ATLAN_SCHEMA = "Schema";
    public static final String ENTITY_TYPE_ATLAN_TABLE  = "Table";
    public static final String ENTITY_TYPE_ATLAN_COLUMN = "Column";

    public static final String RESOURCE_ENTITY_TYPE                   = "entity-type";
    public static final String RESOURCE_ENTITY_ID                     = "entity";

    public static final String ATLAN_DELIMITER = "/";
    public static final String ACCEPT_DEPENDENTS = "/*";
    public static final String ASTERISK = "*";

    public static final String[] SUPPORTED_ENTITY_TYPES = { ENTITY_TYPE_ATLAN_DB, ENTITY_TYPE_ATLAN_SCHEMA, ENTITY_TYPE_ATLAN_TABLE, ENTITY_TYPE_ATLAN_COLUMN };

    public AtlanHekaResourceMapper(){
        super(HEKA_SERVICE_NAME, SUPPORTED_ENTITY_TYPES);
    }

    public AtlanHekaResourceMapper(String componentName, String[] supportedEntityTypes) {
        super(componentName, supportedEntityTypes);
    }


    @Override
    public RangerServiceResource buildResource(RangerAtlasEntity entity) throws Exception {

        List<String> entityIds = new ArrayList<String>();
        String qualifiedName = (String)entity.getAttributes().get(AtlasResourceMapper.ENTITY_ATTRIBUTE_QUALIFIED_NAME);

        entityIds.add(qualifiedName);
        entityIds.add(qualifiedName + ACCEPT_DEPENDENTS);


        if (StringUtils.isEmpty(qualifiedName)) {
            throw new Exception("attribute '" +  ENTITY_ATTRIBUTE_QUALIFIED_NAME + "' not found in entity");
        }

        String resourceStr = getResourceNameFromQualifiedName(qualifiedName);

        if (StringUtils.isEmpty(resourceStr)) {
            throwExceptionWithMessage("resource not found in attribute '" +  ENTITY_ATTRIBUTE_QUALIFIED_NAME + "': " + qualifiedName);
        }

        String   entityType  = entity.getTypeName();
        String   entityId = (String)entity.getAttributes().get(AtlasResourceMapper.ENTITY_ATTRIBUTE_QUALIFIED_NAME);

        List<AtlasClassification>  entityClassifications =  entity.getTags();


        List<String> classificationNamesList = new ArrayList<>();

        if (entityClassifications != null) {
            for (AtlasClassification classification : entityClassifications) {
                classificationNamesList.add(classification.getTypeName());
            }
        }

        String  entityGuid  = entity.getGuid();
        String  serviceName = HEKA_SERVICE_NAME;

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
        if  (StringUtils.isNotEmpty(entityType) && StringUtils.isNotEmpty(qualifiedName) && !classificationNamesList.isEmpty()) {
            elements.put(RESOURCE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource(ASTERISK));
            elements.put(RESOURCE_ENTITY_ID, new RangerPolicy.RangerPolicyResource(entityIds, null, null));
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
}
