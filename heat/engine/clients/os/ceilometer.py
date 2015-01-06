#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from ceilometerclient import client as cc
from ceilometerclient import exc
from ceilometerclient.openstack.common.apiclient import exceptions as api_exc
from oslo.config import cfg

from heat.engine.clients import client_plugin


class CeilometerClientPlugin(client_plugin.ClientPlugin):

    name = 'ceilometer'
    exceptions_module = [exc, api_exc]

    def _create(self):

        con = self.context
        endpoint_type = cfg.CONF.clients_ceilometer.endpoint_type
        endpoint = self.url_for(service_type='metering',
                                endpoint_type=endpoint_type)
        args = {
            'auth_url': con.auth_url,
            'service_type': 'metering',
            'project_id': con.tenant,
            'token': lambda: self.auth_token,
            'endpoint_type': endpoint_type,
            'cacert': cfg.CONF.clients_ceilometer.ca_file,
            'cert_file': cfg.CONF.clients_ceilometer.cert_file,
            'key_file': cfg.CONF.clients_ceilometer.key_file,
            'insecure': cfg.CONF.clients_ceilometer.insecure,
        }

        return cc.Client('2', endpoint, **args)

    def is_not_found(self, ex):
        return isinstance(ex, (exc.HTTPNotFound, api_exc.NotFound))

    def is_over_limit(self, ex):
        return isinstance(ex, exc.HTTPOverLimit)

    def is_conflict(self, ex):
        return isinstance(ex, exc.HTTPConflict)
