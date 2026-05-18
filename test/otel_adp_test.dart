// Licensed under the Dartastic Pro Software License Agreement.
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';
import 'package:dartastic_opentelemetry/testing.dart';
import 'package:otel_adp/otel_adp.dart';
import 'package:test/test.dart';

void main() {
  late TestHarness harness;
  late InMemorySpanExporter spans;

  setUpAll(() async {
    harness = await maybeInitializeOtelForTest(serviceName: 'otel_adp-test');
    spans = harness.spans;
  });

  setUp(() => harness.clear());

  Map<String, Object> attrsOf(Span s) => {
    for (final a in s.attributes.toList()) a.key: a.value,
  };

  test('workers.list records product + region + counts', () async {
    await tracedAdp<String>(
      operation: 'workers.list',
      product: 'wfn',
      region: 'us',
      invoke: () async => const AdpCallResult(
        value: 'ok',
        result: AdpResult(
          itemCount: 100,
          workerCount: 1200,
          product: 'wfn',
          region: 'us',
          statusCode: 200,
        ),
      ),
    );
    final a = attrsOf(spans.findSpanByName('adp workers.list')!);
    expect(a['adp.product'], 'wfn');
    expect(a['adp.region'], 'us');
    expect(a['adp.worker_count'], 1200);
  });

  test('denied event sets span error', () async {
    await tracedAdp<String>(
      operation: 'events.notify',
      invoke: () async => const AdpCallResult(
        value: 'ok',
        result: AdpResult(eventName: 'worker.hire.add', eventOutcome: 'denied'),
      ),
    );
    expect(
      spans.findSpanByName('adp events.notify')!.status,
      SpanStatusCode.Error,
    );
  });

  test('data_connect stream id recorded', () async {
    await tracedAdp<String>(
      operation: 'data_connect.streams.poll',
      product: 'data_connect',
      invoke: () async => const AdpCallResult(
        value: 'ok',
        result: AdpResult(dataConnectStreamId: 'sub-1', statusCode: 200),
      ),
    );
    final a = attrsOf(spans.findSpanByName('adp data_connect.streams.poll')!);
    expect(a['adp.data_connect.stream.id'], 'sub-1');
  });

  test('suppression skips spans', () async {
    await runWithoutAdpInstrumentationAsync(() async {
      await tracedAdp<String>(
        operation: 'workers.list',
        invoke: () async => const AdpCallResult(value: 'v'),
      );
    });
    expect(spans.findSpansStartingWith('adp '), isEmpty);
  });
}
