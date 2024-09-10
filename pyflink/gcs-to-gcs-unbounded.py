################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import argparse
import logging
import sys

from pyflink.common import Row
from pyflink.table import (EnvironmentSettings, TableEnvironment, TableDescriptor, Schema,
                           DataTypes, FormatDescriptor)
from pyflink.table.expressions import lit, col
from pyflink.table.udf import udtf

def word_count(input_path, output_path):
  t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

  # define the source
  t_env.create_temporary_table(
    'source',
    TableDescriptor.for_connector('filesystem')
    .schema(Schema.new_builder()
            .column('word', DataTypes.STRING())
            .build())
    .option('path', input_path)
    .format('raw')
    .build())
  tab = t_env.from_path('source')

  # define the sink
  t_env.create_temporary_table(
  'sink',
  TableDescriptor.for_connector('filesystem')
  .schema(Schema.new_builder()
          .column('word', DataTypes.STRING())
          .column('count', DataTypes.BIGINT())
          .build())
  .option('path', output_path)
  .format(FormatDescriptor.for_format('canal-json')
          .build())
  .build())

  @udtf(result_types=[DataTypes.STRING()])
  def split(line: Row):
    for s in line[0].split(","):
      if len(s) > 1:
        yield Row(s)

  # compute word count
  tab.flat_map(split).alias('word') \
    .group_by(col('word')) \
    .select(col('word'), lit(1).count) \
    .execute_insert('sink') \
    .wait()


if __name__ == '__main__':
  logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

  parser = argparse.ArgumentParser()
  parser.add_argument(
    '--input',
    dest='input',
    required=True,
    help='Input file to process.')
  parser.add_argument(
    '--output',
    dest='output',
    required=True,
    help='Output file to write results to.')

  argv = sys.argv[1:]
  known_args, _ = parser.parse_known_args(argv)

  word_count(known_args.input, known_args.output)