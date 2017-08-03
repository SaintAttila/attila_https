import unittest

from attila.configurations import get_attila_config_manager
from attila.fs import Path


class TestPlugin(unittest.TestCase):

    def test_config_loader(self):
        # TODO: This only tests the get/download functionality, not the put/upload functionality.

        ip = 'api.processplan.com'
        bare_path = '/3.0/processtemplate/15333/instancetask/list/completed'
        query_string = '?offset=0&u=shazia.aftab@ericsson.com&apw=NTE5NF9WdXczMmZFbTV5SHp6QXJNVDU'

        config_manager = get_attila_config_manager()
        remote_path = config_manager.load_path('https://{ip}{bare_path}{query_string}'.format(
            ip=ip,
            bare_path=bare_path,
            query_string=query_string
        ))

        test_results_folder = Path('./test_results')

        print("Opening connections...")
        with remote_path.connection, test_results_folder.connection:
            test_results_folder.make_dir(overwrite=True, clear=True, fill=True)
            self.assertTrue(test_results_folder.exists)
            self.assertTrue(test_results_folder.is_dir)
            self.assertTrue(remote_path.exists)

            print("Copying file to local...")
            results_file = test_results_folder[remote_path.name]
            remote_path.copy_into(test_results_folder, overwrite=True, clear=True, fill=True)
            self.assertTrue(results_file.exists)
            self.assertTrue(results_file.is_file)

            print("Reading remote...")
            with remote_path.open('rb') as file:
                remote = file.read()

            print("Reading results...")
            with results_file.open('rb') as file:
                results = file.read()

            print("Comparing...")
            self.assertEqual(remote, results)

            print("Cleaning up...")

            results_file.remove()
            self.assertFalse(results_file.exists)

            print("Done.")
