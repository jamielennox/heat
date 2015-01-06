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
import six

from keystoneclient.auth.identity import base
from keystoneclient import session
from oslo.config import cfg


# FIXME(jamielennox): I copied this out of a review that is proposed against
# keystoneclient which can be used when available.
class _AccessInfoPlugin(base.BaseIdentityPlugin):
    """A plugin that turns an existing AccessInfo object into a usable plugin.

    In certain circumstances you already have an auth_ref/AccessInfo object
    that you just want to reuse. This could have been from a cache, in
    auth_token middleware or other.

    Turn that existing object into a simple identity plugin. This plugin cannot
    be refreshed as the AccessInfo object does not contain any authorizing
    information.

    :param auth_ref: the existing AccessInfo object.
    :type auth_ref: keystoneclient.access.AccessInfo
    :param auth_url: the url where this AccessInfo was retrieved from. Required
                     if using the AUTH_INTERFACE with get_endpoint. (optional)
    """

    def __init__(self, auth_url, auth_ref):
        super(_AccessInfoPlugin, self).__init__(auth_url=auth_url,
                                                reauthenticate=False)
        self.auth_ref = auth_ref

    def get_auth_ref(self, session, **kwargs):
        return self.auth_ref

    def invalidate(self):
        # NOTE(jamielennox): Don't allow the default invalidation to occur
        # because on next authentication request we will only get the same
        # auth_ref object again.
        return False


@six.add_metaclass(abc.ABCMeta)
class ClientPlugin(object):

    # Module which contains all exceptions classes which the client
    # may emit
    exceptions_module = None

    def __init__(self, context):
        self.context = context
        self.clients = context.clients
        self._client = None
        self._session = None

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
        return self.context.auth_plugin.get_token(self.get_session('keystone'))

    def url_for(self, **kwargs):
        # NOTE(jamielennox): use the session defined by the keystoneclient
        # options as traditionally the token was always retrieved from
        # keystoneclient.

        try:
            kwargs['interface'] = kwargs.pop('endpoint_type')
        except KeyError:
            pass

        session = self.get_session('keystone')
        return self.context.auth_plugin.get_endpoint(session, **kwargs)

    def get_session(self, name):
        if not self._session:
            group = 'clients_%s' % name

            self._session = session.Session.construct({
                'cacert': cfg.CONF[group].ca_file,
                'cert': cfg.CONF[group].cert_file,
                'key': cfg.CONF[group].key_file,
                'insecure': cfg.CONF[group].insecure,
            })

        return self._session

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
