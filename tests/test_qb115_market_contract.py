"""Release manifest contract; pure stdlib, no MoviePilot imports."""
import ast
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]


class Qb115MarketContract(unittest.TestCase):
    def test_market_entry_matches_versioned_plugin(self):
        market = json.loads((ROOT / 'package.v3.json').read_text())
        self.assertIn('Qb115Downloader', market)
        entry = market['Qb115Downloader']
        source = ast.parse((ROOT / 'plugins.v3/qb115downloader/__init__.py').read_text())
        cls = next(n for n in source.body if isinstance(n, ast.ClassDef) and n.name == 'Qb115Downloader')
        values = {n.targets[0].id: ast.literal_eval(n.value) for n in cls.body
                  if isinstance(n, ast.Assign) and isinstance(n.targets[0], ast.Name) and isinstance(n.value, ast.Constant)}
        self.assertEqual(entry['name'], values['plugin_name'])
        self.assertEqual(entry['version'], values['plugin_version'])
        self.assertEqual(entry['version'], '0.1.1')
        self.assertIn('v0.1.0', entry['history'])
        self.assertIn('v' + entry['version'], entry['history'])
        readme = (ROOT / 'plugins.v3/qb115downloader/README.md').read_text()
        self.assertIn('`0.1.1`', readme)
        self.assertIn('legacy', readme)
        self.assertIn('完整 hash', readme)
        self.assertEqual(entry['system_version'], '>=3.0.0')
        self.assertNotIn('release', entry)


if __name__ == '__main__':
    unittest.main()
