package org.anish.spark.indexeddedup.internal

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

/**
  * Created by anish on 07/05/17.
  */
private[indexeddedup] object SerializableHelpers {
  def getKeysFromRow(row: GenericRowWithSchema, primaryKeys: Seq[String]): Seq[String] =
    primaryKeys.map(fieldName => row.getAs[String](fieldName))
}
