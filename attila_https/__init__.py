"""
Requests-based Attila plugin for HTTPS path support.
"""

from typing import Optional
import logging
import os
import warnings

from urllib.parse import urlparse

import requests

from attila.configurations import ConfigManager
from attila.abc.files import FSConnector, Path, fs_connection
from attila.exceptions import verify_type, OperationNotSupportedError
# from attila.security import credentials
from attila import strings
from attila.fs import local
from attila.fs.proxies import ProxyFile


__author__ = 'Aaron Hosford'
__author_email__ = 'aaron.hosford@ericsson.com'
__version__ = '0.0'
__url__ = 'https://scmgr.eams.ericsson.net/PythonLibs/attila_https'
__description__ = 'Requests-based Attila plugin for HTTPS support'
__license__ = 'MIT'
__long_description__ = __doc__
__install_requires__ = ['attila>=1.10.5', 'requests']

# This tells Attila how to find our plugins.
__entry_points__ = {
    'attila.config_loader': [
        'HTTPSConnector = attila_https:HTTPSConnector',
        'https_connection = attila_https:https_connection',
    ],
    'attila.url_scheme': [
        'https = attila_https:HTTPSConnector',
    ]
}


__all__ = [
    'DEFAULT_HTTPS_PORT',
    'HTTPSConnector',
    'https_connection',
]


log = logging.getLogger(__name__)


DEFAULT_HTTPS_PORT = 443
HTTPS_URL_TEMPLATE = 'https://{server}{path}'
HTTPS_URL_PORT_TEMPLATE = 'https://{server}:{port}{path}'


class HTTPSConnector(FSConnector):
    """
    Stores the HTTPS connection information as a single object which can then be passed around
    instead of using multiple parameters to a function.
    """

    @classmethod
    def load_url(cls, manager, url):
        """
        Load a new Path instance from a URL string.

        The standard format for an HTTPS URL is "https://host:port/path".

        :param manager: The ConfigManager instance.
        :param url: The URL to load.
        :return: The resultant Path instance.
        """
        verify_type(manager, ConfigManager)
        verify_type(url, str)

        if '://' not in url:
            url = 'https://' + url
        scheme, netloc, path, params, query, fragment = urlparse(url)
        assert not params and not fragment
        assert scheme.lower() == 'https'
        assert '@' not in netloc, "Embedded credentials not currently supported for HTTPS."

        # TODO: Support login credentials someday?
        # user, address = netloc.split('@')
        # # We do not permit passwords to be stored in plaintext in the parameter value.
        # assert ':' not in user

        address = netloc

        if ':' in address:
            server, port = address.split(':')
            port = int(port)
        else:
            server = address
            port = DEFAULT_HTTPS_PORT

        # credential_string = '%s@%s/https' % (user, server)
        # credential = manager.load_value(credential_string, credentials.Credential)

        return Path(path + '?' + query, cls('%s:%s' % (server, port)).connect())

    @classmethod
    def load_config_section(cls, manager, section, *args, **kwargs):
        """
        Load a new instance from a config section on behalf of a config loader.

        :param manager: An attila.configurations.ConfigManager instance.
        :param section: The name of the section being loaded.
        :return: An instance of this type.
        """
        verify_type(manager, ConfigManager)
        assert isinstance(manager, ConfigManager)

        verify_type(section, str, non_empty=True)

        server = manager.load_option(section, 'Server', str)
        port = manager.load_option(section, 'Port', int, None)
        # credential = manager.load_section(section, credentials.Credential)

        if port is not None:
            server = '%s:%s' % (server, port)

        return super().load_config_section(
            manager,
            section,
            *args,
            server=server,
            # credential=credential,
            **kwargs
        )

    def __init__(self, server, initial_cwd=None):
        verify_type(server, str, non_empty=True)
        server, port = strings.split_port(server, DEFAULT_HTTPS_PORT)

        super().__init__(https_connection, initial_cwd)

        self._server = server
        self._port = port

    def __repr__(self):
        server_string = None
        if self._server is not None:
            if self._port == DEFAULT_HTTPS_PORT:
                server_string = self._server
            else:
                server_string = '%s:%s' % (self._server, self._port)
        args = [repr(server_string), repr(self.initial_cwd)]
        return type(self).__name__ + '(' + ', '.join(args) + ')'

    @property
    def server(self) -> str:
        """The DNS name or IP address of the remote server."""
        return self._server

    @property
    def port(self):
        """The remote server's port."""
        return self._port

    def connect(self):
        """Create a new connection and return it."""
        return super().connect()


# noinspection PyPep8Naming
class https_connection(fs_connection):
    """
    An https_connection manages the state for a connection to an HTTPS server, providing a
    standardized interface for interacting with remote files and directories.
    """

    @classmethod
    def get_connector_type(cls):
        """Get the connector type associated with this connection type."""
        return HTTPSConnector

    def __init__(self, connector: HTTPSConnector):
        """
        Create a new https_connection instance.

        Example:
            # Get a connection to the HTTPS server.
            connection = https_connection(connector)
        """
        assert isinstance(connector, HTTPSConnector)
        super().__init__(connector)

    def name(self, path) -> str:
        """
        Get the name of the file system object.

        :param path: The path to operate on.
        :return: The name.
        """
        path = self.check_path(path)
        raw_path = urlparse(path)[2]  # Remove the query string, etc.
        return os.path.basename(raw_path)

    def dir(self, path) -> Optional[Path]:
        """
        Get the parent directory of the file system object.

        :param path: The path to operate on.
        :return: The parent directory's path, or None.
        """
        path = self.check_path(path)
        raw_path = urlparse(path)[2]  # Remove the query string, etc.
        dir_path = os.path.dirname(raw_path)
        if dir_path == raw_path:
            return None
        else:
            return Path(dir_path, self)

    def open(self):
        """Open the HTTPS connection."""
        assert not self.is_open

        cwd = self.getcwd()

        super().open()
        if cwd is None:
            # This forces the CWD to be refreshed.
            self.getcwd()
        else:
            # This overrides the CWD based on what it was set to before the connection was opened.
            self.chdir(cwd)

    def close(self):
        """Close the HTTPS connection"""
        if not self._is_open:
            warnings.warn("Double-closing HTTPS connection.")
        self._is_open = False

    def chdir(self, path):
        """Set the current working directory of this HTTPS connection."""
        super().chdir(path)

    def _get_url(self, path: str) -> str:
        if self._connector.port == DEFAULT_HTTPS_PORT:
            return HTTPS_URL_TEMPLATE.format(
                server=self._connector.server,
                path=str(path)
            )
        else:
            return HTTPS_URL_PORT_TEMPLATE.format(
                server=self._connector.server,
                port=self._connector.port,
                path=str(path)
            )

    def _download(self, remote_path, local_path):
        assert self.is_open
        remote_path = self.check_path(remote_path)
        assert isinstance(local_path, str)

        response = requests.get(self._get_url(remote_path))
        response.raise_for_status()
        with open(local_path, 'wb') as local_copy:
            local_copy.writelines(response.iter_content())

    def _upload(self, local_path, remote_path):
        assert self.is_open

        if isinstance(local_path, Path):
            assert isinstance(local_path.connection, local.local_fs_connection)
            local_path = str(local_path)
        assert isinstance(local_path, str)

        remote_path = self.check_path(remote_path)

        with open(local_path, 'rb') as local_copy:
            response = requests.put(remote_path, local_copy)
            response.raise_for_status()

    def open_file(self, path, mode='r', buffering=-1, encoding=None, errors=None, newline=None, closefd=True,
                  opener=None):
        """
        Open the file.

        :param path: The path to operate on.
        :param mode: The file mode.
        :param buffering: The buffering policy.
        :param encoding: The encoding.
        :param errors: The error handling strategy.
        :param newline: The character sequence to use for newlines.
        :param closefd: Whether to close the descriptor after the file closes.
        :param opener: A custom opener.
        :return: The opened file object.
        """
        assert self.is_open

        mode = mode.lower()
        path = self.check_path(path)

        # We can't work directly with an HTTPS file. Instead, we will create a temp file and return it
        # as a proxy.
        with local.local_fs_connection() as connection:
            temp_path = str(abs(connection.get_temp_file_path(self.name(path))))

        # If we're not truncating the file, then we'll need to copy down the data.
        if mode not in ('w', 'wb'):
            self._download(path, temp_path)

        if mode in ('r', 'rb'):
            writeback = None
        else:
            writeback = self._upload

        return ProxyFile(Path(path, self), mode, buffering, encoding, errors, newline, closefd,
                         opener, proxy_path=temp_path, writeback=writeback)

    def list(self, path, pattern='*'):
        """
        Return a list of the names of the files and directories appearing in this folder.

        :param path: The path to operate on.
        :param pattern: A glob-style pattern against which names must match.
        :return: A list of matching file and directory names.
        """
        raise OperationNotSupportedError()

    def size(self, path):
        """
        Get the size of the file.

        :param path: The path to operate on.
        :return: The size in bytes.
        """
        raise OperationNotSupportedError()

    def modified_time(self, path):
        """
        Get the last time the data of file system object was modified.

        :param path: The path to operate on.
        :return: The time stamp, as a float.
        """
        raise OperationNotSupportedError()

    def remove(self, path):
        """
        Remove the folder or file.

        :param path: The path to operate on.
        """
        assert self.is_open
        path = self.check_path(path)

        if self.is_dir(path):
            raise OperationNotSupportedError()
        else:
            response = requests.delete(self._get_url(path))
            response.raise_for_status()

    def make_dir(self, path, overwrite=False, clear=False, fill=True, check_only=None):
        """
        Create a directory at this location.

        :param path: The path to operate on.
        :param overwrite: Whether existing files/folders that conflict with this function are to be
            deleted/overwritten.
        :param clear: Whether the directory at this location must be empty for the function to be
            satisfied.
        :param fill: Whether the necessary parent folder(s) are to be created if the do not exist
            already.
        :param check_only: Whether the function should only check if it's possible, or actually
            perform the operation.
        :return: None
        """
        raise OperationNotSupportedError()

    def rename(self, path, new_name):
        """
        Rename a file object.

        :param path: The path to be operated on.
        :param new_name: The new name of the file object, as as string.
        :return: None
        """
        raise OperationNotSupportedError()

    def is_dir(self, path):
        """
        Determine if the path refers to an existing directory.

        :param path: The path to operate on.
        :return: Whether the path is a directory.
        """
        return False

    def is_file(self, path):
        """
        Determine if the path refers to an existing file.

        :param path: The path to operate on.
        :return: Whether the path is a file.
        """
        path = self.check_path(path)
        return requests.get(self._get_url(path)).ok

    def join(self, *path_elements):
        """
        Join several path elements together into a single path.

        :param path_elements: The path elements to join.
        :return: The resulting path.
        """
        if path_elements:
            # There is a known Python bug which causes any TypeError raised by a generator during
            # argument interpolation with * to be incorrectly reported as:
            #       TypeError: join() argument after * must be a sequence, not generator
            # The bug is documented at:
            #       https://mail.python.org/pipermail/new-bugs-announce/2009-January.txt
            # To avoid this confusing misrepresentation of errors, I have broken this section out
            # into multiple statements so TypeErrors get the opportunity to propagate correctly.
            starting_slash = path_elements and str(path_elements[0]).startswith('/')
            path_elements = tuple(self.check_path(element).strip('/\\') for element in path_elements)
            if starting_slash:
                path_elements = ('',) + path_elements
            return Path('/'.join(path_elements), connection=self)
        else:
            return Path(connection=self)
