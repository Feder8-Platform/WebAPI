/*
 *
 * Copyright 2017 Observational Health Data Sciences and Informatics
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.ohdsi.webapi.cdmresults.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.ohdsi.webapi.cdmresults.DescendantRecordCount;

public class DescendantRecordCountMapper {

	public DescendantRecordCount mapRow(ResultSet rs)
			throws SQLException {
		DescendantRecordCount descendantRecordCount = new DescendantRecordCount();
		descendantRecordCount.setId(rs.getInt("concept_id"));
		descendantRecordCount.setRecordCount(rs.getLong("record_count"));
		descendantRecordCount.setDescendantRecordCount(rs.getLong("descendant_record_count"));
		descendantRecordCount.setPersonRecordCount(rs.getLong("person_record_count"));
		return descendantRecordCount;
	}
}
