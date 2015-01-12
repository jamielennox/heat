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

import abc

from keystoneclient import session
from oslo.config import cfg
import six


@six.add_metaclass(abc.ABCMeta)
class ClientPlugin(object):

    # Module which contains all exceptions classes which the client
    # may emit
    exceptions_module = None

    def __init__(self, context):
        self.context = context
        self.clients = context.clients
        self._client = None

    def client(self):
        if not self._client:
            self._client = self._create()
        return self._client

    @abc.abstractmethod
    def _create(self):
        '''Return a newly created client.'''
        pass

    @property
    def auth_token(self):
        # NOTE(jamielennox): use the session defined by the keystoneclient
        # options as traditionally the token was always retrieved from
        # keystoneclient.
        session = self.clients.client('keystone').session
        return self.context.auth_plugin.get_token(session)

    def url_for(self, **kwargs):
        # NOTE(jamielennox): use the session defined by the keystoneclient
        # options as traditionally the token was always retrieved from
        # keystoneclient.
        try:
            kwargs['interface'] = kwargs.pop('endpoint_type')
        except KeyError:
            pass

        session = self.clients.client('keystone').session
        return self.context.auth_plugin.get_endpoint(session, **kwargs)

    def _get_client_option(self, client, option):
        # look for the option in the [clients_${client}] section
        # unknown options raise cfg.NoSuchOptError
        try:
            group_name = 'clients_' + client
            cfg.CONF.import_opt(option, 'heat.common.config',
                                group=group_name)
            v = getattr(getattr(cfg.CONF, group_name), option)
            if v is not None:
                return v
        except cfg.NoSuchGroupError:
            pass  # do not error if the client is unknown
        # look for the option in the generic [clients] section
        cfg.CONF.import_opt(option, 'heat.common.config', group='clients')
        return getattr(cfg.CONF.clients, option)

    def is_client_exception(self, ex):
        '''Returns True if the current exception comes from the client.'''
        if self.exceptions_module:
            if isinstance(self.exceptions_module, list):
                for m in self.exceptions_module:
                    if type(ex) in m.__dict__.values():
                        return True
            else:
                return type(ex) in self.exceptions_module.__dict__.values()
        return False

    def is_not_found(self, ex):
        '''Returns True if the exception is a not-found.'''
        return False

    def is_over_limit(self, ex):
        '''Returns True if the exception is an over-limit.'''
        return False

    def ignore_not_found(self, ex):
        '''Raises the exception unless it is a not-found.'''
        if not self.is_not_found(ex):
            raise ex

    def _get_session(self, client):
        return session.Session.construct({
            'cacert': self._get_client_option(client, 'ca_file'),
            'insecure': self._get_client_option(client, 'insecure'),
            'cert': self._get_client_option(client, 'cert_file'),
            'key': self._get_client_option(client, 'key_file'),
        })
