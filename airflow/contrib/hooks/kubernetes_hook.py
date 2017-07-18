# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import requests

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook

from kubernetes import client, config

class KubernetesHook(BaseHook):
    """
    Kubernetes interaction hook

    :param k8s_conn_id: reference to a pre-defined K8s Connection
    :type k8s_conn_id: string
    """
    def __init__(self, k8s_conn_id="k8s_default"):
        self.conn_id = k8s_conn_id
        self.core_client = None

    def get_conn(self):
        """
        Initializes the api client. Only config file or env
        configuration supported at the moment.
        """
        if not self.core_client:
            config.load_kube_config()
            self.core_client = client.CoreV1Api()

        return self.core_client

    def get_env_definitions(self, env):
        def get_env(name, definition):
            if isinstance(definition, str):
                return client.V1EnvVar(name=name, value=definition)
            elif isinstance(definition, dict):
                source = definition['source']
                if source == 'configMap':
                    return client.V1EnvVar(name=name,
                            value_from=client.V1EnvVarSource(
                                config_map_key_ref=client.V1ConfigMapKeySelector(
                                    key=definition['key'], name=definition['name'])))
                elif source == 'secret':
                    return client.V1EnvVar(name=name,
                            value_from=client.V1EnvVarSource(
                                secret_key_ref=client.V1SecretKeySelector(
                                    key=definition['key'], name=definition['name'])))
                else:
                    raise AirflowException('Creating env vars from %s not yet implemented', source)
            else:
                raise AirflowException('Environment variable definition \
                    has to be either string or a dictionary. %s given instead', type(definition))

        return [get_env(name, definition) for name, definition in env.items()]

    """
        Builds pod definition based on supplied arguments
    """
    def get_pod_definition(
            self,
            image,
            name,
            namespace=None,
            restart_policy="Never",
            command=None,
            args=None,
            env=None,
            labels=None):
        env_defs = self.get_env_definitions(env) if env else None
        return client.V1Pod(
            api_version="v1",
            kind="Pod",
            metadata=client.V1ObjectMeta(
                name=name,
                namespace=namespace,
                labels=labels
            ),
            spec=client.V1PodSpec(
                restart_policy=restart_policy,
                containers=[client.V1Container(
                    name=name,
                    command=command,
                    args=args,
                    image=image,
                    env=env_defs
                )]
            )
        )

    def create_pod(self, pod):
        namespace = pod.metadata.namespace
        self.get_conn().create_namespaced_pod(namespace, pod)

    """
        Delete a pod based on pod definition or name
    """
    def delete_pod(
            self,
            pod=None,
            name=None,
            namespace=None):
        if pod:
            name = pod.metadata.name
            namespace = pod.metadata.namespace
        self.get_conn().delete_namespaced_pod(name, namespace, client.V1DeleteOptions())

    """
        Fetches pod status and returns phase
    """
    def get_pod_state(self, pod=None, name=None, namespace=None):
        if pod:
            name = pod.metadata.name
            namespace = pod.metadata.namespace

        pod_status = self.get_conn().read_namespaced_pod_status(name, namespace)

        if not pod_status:
            raise AirflowException("Cannot find the requested pod!")

        return pod_status.status.phase

    def relay_pod_logs(self, pod=None, name=None, namespace=None):
        if pod:
            name = pod.metadata.name
            namespace = pod.metadata.namespace

        if not self._stream_log(name, namespace):
            self._api_log(name, namespace)


    """
        Stream logs for pod.
        The python-client for kubernetes does not (yet) support iterating over a
        streaming log.

        Only bearer authenticated requests for now.
        (Which is enough if running the worker in kubernetes)
    """
    def _stream_log(self, name, namespace):
        if client.configuration.api_key['authorization'] is not None:
            headers = {'Authorization':client.configuration.api_key['authorization']}
        else:
            return None

        r = requests.get("%s/api/v1/namespaces/%s/pods/%s/log" %
                            (client.configuration.host, namespace, name),
                         params={'follow':'true'},
                         verify=client.configuration.ssl_ca_cert,
                         headers=headers,
                         stream=True)

        if r.encoding is None:
            r.encoding = 'utf-8'

        for line in r.iter_lines(decode_unicode=True):
            if not line:
                continue

            logging.info(line.strip())

        return True

    """
        Fetch log from k8s client.
        read_namespaced_pod_log with follow=True, only returns once the log is closed.
    """
    def _api_log(self, name, namespace):
        log = self.get_conn().read_namespaced_pod_log(
                        name,
                        namespace,
                        follow=True)

        log_lines = log.rstrip().split("\n")
        for line in log_lines:
            logging.info(line.rstrip())
