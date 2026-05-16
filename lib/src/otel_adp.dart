// Licensed under the Apache License, Version 2.0
// Copyright 2025, Mindful Software LLC, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';

import 'otel_adp_suppression.dart';

const _tracerName = 'dartastic_otel_libs_adp';

Tracer _tracer() => OTel.tracerProvider().getTracer(_tracerName);

class AdpResult {
  const AdpResult({
    this.itemCount,
    this.workerCount,
    this.product,
    this.region,
    this.eventName,
    this.eventOutcome,
    this.payrollPeriodId,
    this.dataConnectStreamId,
    this.requestId,
    this.statusCode,
  });

  /// `meta.totalNumberOfRecords` or `workers.length`.
  final int? itemCount;

  /// `meta.totalNumberOfWorkers` when distinct from itemCount.
  final int? workerCount;

  /// `wfn` (Workforce Now) / `run` / `data_connect` / `vantage` —
  /// each has different rate limits and auth.
  final String? product;

  /// `us` / `ca` / `eu` — ADP regional API edges.
  final String? region;

  /// `worker.hire.add` / `worker.terminate` / `pay-distribution.modify`
  /// etc. — ADP event verbs are the API surface for writes.
  final String? eventName;

  /// `confirmed` / `denied` / `pending` / `failed` from event
  /// responses.
  final String? eventOutcome;

  /// Pay period ID for payroll calls.
  final String? payrollPeriodId;

  /// `subscriptionID` for DataConnect streaming endpoints.
  final String? dataConnectStreamId;

  /// `Correlation-Id` header.
  final String? requestId;

  final int? statusCode;
}

class AdpCallResult<T> {
  const AdpCallResult({required this.value, this.result});
  final T value;
  final AdpResult? result;
}

/// Wraps an ADP API call in a CLIENT span. [operation] is one of
/// `workers.list`, `workers.get`, `workers.hire`, `workers.terminate`,
/// `workers.modify`, `time_cards.list`, `time_cards.approve`,
/// `pay_distributions.list`, `pay_distributions.modify`,
/// `pay_statements.list`, `pay_data.list`, `data_connect.streams.list`,
/// `data_connect.streams.poll`, `events.notify`.
Future<T> tracedAdp<T>({
  required String operation,
  required Future<AdpCallResult<T>> Function() invoke,
  String? product,
  String? region,
}) async {
  if (adpInstrumentationSuppressed()) {
    final r = await invoke();
    return r.value;
  }
  final m = <String, Object>{
    'rpc.system': 'adp',
    'rpc.method': operation,
    'adp.operation': operation,
  };
  if (product != null) m['adp.product'] = product;
  if (region != null) m['adp.region'] = region;

  final span = _tracer().startSpan(
    'adp $operation',
    kind: SpanKind.client,
    attributes: OTel.attributesFromMap(m),
  );
  try {
    final r = await invoke();
    if (r.result != null) {
      final extras = <Attribute<Object>>[];
      if (r.result!.itemCount != null) {
        extras.add(OTel.attributeInt('adp.item_count', r.result!.itemCount!));
      }
      if (r.result!.workerCount != null) {
        extras.add(
          OTel.attributeInt('adp.worker_count', r.result!.workerCount!),
        );
      }
      if (r.result!.product != null) {
        extras.add(OTel.attributeString('adp.product', r.result!.product!));
      }
      if (r.result!.region != null) {
        extras.add(OTel.attributeString('adp.region', r.result!.region!));
      }
      if (r.result!.eventName != null) {
        extras.add(
          OTel.attributeString('adp.event.name', r.result!.eventName!),
        );
      }
      if (r.result!.eventOutcome != null) {
        extras.add(
          OTel.attributeString('adp.event.outcome', r.result!.eventOutcome!),
        );
        if (r.result!.eventOutcome == 'failed' ||
            r.result!.eventOutcome == 'denied') {
          span.setStatus(
            SpanStatusCode.Error,
            'ADP event ${r.result!.eventOutcome}',
          );
        }
      }
      if (r.result!.payrollPeriodId != null) {
        extras.add(
          OTel.attributeString(
            'adp.payroll.period.id',
            r.result!.payrollPeriodId!,
          ),
        );
      }
      if (r.result!.dataConnectStreamId != null) {
        extras.add(
          OTel.attributeString(
            'adp.data_connect.stream.id',
            r.result!.dataConnectStreamId!,
          ),
        );
      }
      if (r.result!.requestId != null) {
        extras.add(
          OTel.attributeString('adp.request.id', r.result!.requestId!),
        );
      }
      if (r.result!.statusCode != null) {
        extras.add(
          OTel.attributeInt('http.response.status_code', r.result!.statusCode!),
        );
        if (r.result!.statusCode! >= 400) {
          span.setStatus(
            SpanStatusCode.Error,
            'ADP HTTP ${r.result!.statusCode}',
          );
        }
      }
      if (extras.isNotEmpty) span.addAttributes(OTel.attributes(extras));
    }
    return r.value;
  } catch (e, st) {
    span.addAttributes(
      OTel.attributes([
        OTel.attributeString(
          ErrorResource.errorType.key,
          e.runtimeType.toString(),
        ),
      ]),
    );
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}
