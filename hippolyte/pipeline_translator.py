import json
from copy import deepcopy

# Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.


class PipelineDefinitionError(Exception):
    def __init__(self, msg):
        full_msg = (
            "Error in pipeline definition: %s\n" % msg)
        super(PipelineDefinitionError, self).__init__(full_msg)
        self.msg = msg


def definition_to_api_objects(definition):
    definition_copy = deepcopy(definition)
    if 'objects' not in definition_copy:
        raise PipelineDefinitionError('Missing "objects" key')
    api_elements = []
    # To convert to the structure expected by the service,
    # we convert the existing structure to a list of dictionaries.
    # Each dictionary has a 'fields', 'id', and 'name' key.
    for element in definition_copy['objects']:
        try:
            element_id = element.pop('id')
        except KeyError:
            raise PipelineDefinitionError('Missing "id" key of element: %s' %
                                          json.dumps(element))
        api_object = {'id': element_id}
        # If a name is provided, then we use that for the name,
        # otherwise the id is used for the name.
        name = element.pop('name', element_id)
        api_object['name'] = name
        # Now we need the field list.  Each element in the field list is a dict
        # with a 'key', 'stringValue'|'refValue'
        fields = []
        for key, value in sorted(element.items()):
            fields.extend(_parse_each_field(key, value))
        api_object['fields'] = fields
        api_elements.append(api_object)
    return api_elements


def definition_to_api_parameters(definition):
    definition_copy = deepcopy(definition)
    if 'parameters' not in definition_copy:
        return None
    parameter_objects = []
    for element in definition_copy['parameters']:
        try:
            parameter_id = element.pop('id')
        except KeyError:
            raise PipelineDefinitionError('Missing "id" key of parameter: %s' %
                                          json.dumps(element))
        parameter_object = {'id': parameter_id}
        # Now we need the attribute list.  Each element in the attribute list
        # is a dict with a 'key', 'stringValue'
        attributes = []
        for key, value in sorted(element.items()):
            attributes.extend(_parse_each_field(key, value))
        parameter_object['attributes'] = attributes
        parameter_objects.append(parameter_object)
    return parameter_objects


def definition_to_parameter_values(definition):
    definition_copy = deepcopy(definition)
    if 'values' not in definition_copy:
        return None
    parameter_values = []
    for key in definition_copy['values']:
        parameter_values.extend(
            _convert_single_parameter_value(key, definition_copy['values'][key]))

    return parameter_values


def _parse_each_field(key, value):
    values = []
    if isinstance(value, list):
        for item in value:
            values.append(_convert_single_field(key, item))
    else:
        values.append(_convert_single_field(key, value))
    return values


def _convert_single_field(key, value):
    field = {'key': key}
    if isinstance(value, dict) and list(value.keys()) == ['ref']:
        field['refValue'] = value['ref']
    else:
        field['stringValue'] = value
    return field


def _convert_single_parameter_value(key, values):
    parameter_values = []
    if isinstance(values, list):
        for each_value in values:
            parameter_value = {'id': key, 'stringValue': each_value}
            parameter_values.append(parameter_value)
    else:
        parameter_value = {'id': key, 'stringValue': values}
        parameter_values.append(parameter_value)
    return parameter_values
