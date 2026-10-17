"""Verifica o fluxo real do notebook com fronteiras de Spark/Delta simuladas."""
import ast
import contextlib
from datetime import datetime, timezone
import io
import json
import logging
from pathlib import Path
from types import SimpleNamespace
import unittest

SOURCE = Path(__file__).resolve().parents[1] / 'gold/gold_bi.py'
TREE = ast.parse(SOURCE.read_text(encoding='utf-8'))


def run_flow(bad_table=None, write_error=None):
    state = SimpleNamespace(events=[], metadata={})
    class Frame:
        def __init__(self, name): self.name, self.options = name, {}
        @property
        def write(self): return self
        def format(self, *args): return self
        def mode(self, *args): return self
        def option(self, key, value): self.options[key] = value; return self
        def saveAsTable(self, target):
            state.events.append(('write', self.name))
            if self.name == write_error: raise RuntimeError('storage failure')
            state.metadata[self.name] = json.loads(self.options['userMetadata'])
    def validate(name, frame):
        state.events.append(('validate', name))
        if name == bad_table: raise ValueError('invalid facts')
        return {'rows': 3, 'state': 'VALID'}
    namespace = {
        'json': json, 'prepared_tables': {}, 'quality_metrics': {},
        'completed_tables': [], 'failed_tables': [], 'validate_bi': validate,
        'BI_BATCH': 'batch', 'BI_CUTOFF': '2026-10-17T12:00:00Z',
        'started_at': datetime(2026, 10, 17, tzinfo=timezone.utc),
        '_source_versions': {'source': 7},
        'dbutils': SimpleNamespace(widgets=SimpleNamespace(get=lambda _: 'Demonstração')),
        'bi_target': lambda schema, name: f'{schema}.{name}',
        'bi_read': lambda name: Frame(name), 'bi_properties': lambda: Frame('properties'),
        'bi_parts': lambda: Frame('parts'), 'bi_user_keys': lambda _: Frame('keys'),
        'logger': logging.getLogger('publication-test'),
    }
    for function, table in (
        ('weather_hourly', 'tb_weather_hourly'), ('consumption_hourly', 'tb_consumption_hourly'),
        ('alert_count_daily', 'tb_alert_count_daily'), ('agent_calls', 'tb_agent_call'),
        ('tariff_validity', 'tb_tariff_validity'), ('target_compliance_daily', 'tb_target_compliance_daily'),
        ('device_availability_daily', 'tb_device_availability_daily'),
    ):
        namespace[function] = lambda *args, table=table: Frame(table)
    definitions = [n for n in TREE.body if isinstance(n, ast.FunctionDef) and n.name in ('save_bi', 'prepare_bi')]
    exec(compile(ast.Module(body=definitions, type_ignores=[]), str(SOURCE), 'exec'), namespace)
    start = next(i for i, n in enumerate(TREE.body) if isinstance(n, ast.Assign)
                 and any(isinstance(t, ast.Name) and t.id == 'table_name' for t in n.targets))
    error = None
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        try: exec(compile(ast.Module(body=TREE.body[start:], type_ignores=[]), str(SOURCE), 'exec'), namespace)
        except RuntimeError as exc: error = exc
    return state, error


class PublicationTests(unittest.TestCase):
    def test_all_seven_are_validated_before_first_analytical_write(self):
        state, error = run_flow()
        self.assertIsNone(error)
        self.assertEqual([e[0] for e in state.events], ['validate'] * 7 + ['write'] * 7)
        for meta in state.metadata.values():
            self.assertEqual(meta['quality_contract_version'], 3)
            self.assertEqual(meta['source_batch_id'], 'batch')
            self.assertEqual(meta['quality']['rows'], 3)
            self.assertEqual(meta['source_versions'], {'source': 7})

    def test_invalid_last_table_prevents_every_analytical_write(self):
        state, error = run_flow(bad_table='tb_device_availability_daily')
        self.assertIsNotNone(error)
        self.assertEqual([e[0] for e in state.events], ['validate'] * 7)
        self.assertEqual(state.metadata, {})

    def test_storage_failure_is_propagated_and_never_reported_as_success(self):
        state, error = run_flow(write_error='tb_agent_call')
        self.assertIn('storage failure', str(error))
        self.assertLess(len(state.metadata), 7)  # Delta não garante transação entre tabelas.


if __name__ == '__main__': unittest.main(verbosity=2)
