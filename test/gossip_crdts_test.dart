import 'dart:async';

import 'package:gossip/gossip.dart';
import 'package:gossip_crdts/gossip_crdts.dart';
import 'package:test/test.dart';

/// Mock transport for testing
class MockTransport implements GossipTransport {
  final String nodeId;
  final Map<String, MockTransport> _network;

  final StreamController<IncomingDigest> _digestController =
      StreamController<IncomingDigest>.broadcast();
  final StreamController<IncomingEvents> _eventsController =
      StreamController<IncomingEvents>.broadcast();

  MockTransport(this.nodeId, this._network);

  @override
  Future<void> initialize() async {
    _network[nodeId] = this;
  }

  @override
  Future<void> shutdown() async {
    _network.remove(nodeId);
    await _digestController.close();
    await _eventsController.close();
  }

  @override
  Future<GossipDigestResponse> sendDigest(
    GossipPeer peer,
    GossipDigest digest, {
    Duration? timeout,
  }) async {
    final targetTransport = _network[peer.id];
    if (targetTransport == null) {
      throw TransportException('Peer ${peer.id} not found');
    }

    final completer = Completer<GossipDigestResponse>();
    final incomingDigest = IncomingDigest(
      fromPeer: GossipPeer(id: nodeId, address: 'mock://$nodeId'),
      digest: digest,
      respond: (response) async {
        completer.complete(response);
      },
    );

    targetTransport._digestController.add(incomingDigest);
    return completer.future;
  }

  @override
  Future<void> sendEvents(
    GossipPeer peer,
    GossipEventMessage message, {
    Duration? timeout,
  }) async {
    final targetTransport = _network[peer.id];
    if (targetTransport == null) {
      throw TransportException('Peer ${peer.id} not found');
    }

    final incomingEvents = IncomingEvents(
      fromPeer: GossipPeer(id: nodeId, address: 'mock://$nodeId'),
      message: message,
    );

    targetTransport._eventsController.add(incomingEvents);
  }

  @override
  Stream<IncomingDigest> get incomingDigests => _digestController.stream;

  @override
  Stream<IncomingEvents> get incomingEvents => _eventsController.stream;

  @override
  Future<List<GossipPeer>> discoverPeers() async {
    return _network.keys
        .where((id) => id != nodeId)
        .map((id) => GossipPeer(id: id, address: 'mock://$id'))
        .toList();
  }

  @override
  Future<bool> isPeerReachable(GossipPeer peer) async {
    return _network.containsKey(peer.id);
  }
}

void main() {
  group('CRDT Operations', () {
    group('GCounter', () {
      test('should create counter with initial value zero', () {
        final counter = GCounter('test-counter');
        expect(counter.id, equals('test-counter'));
        expect(counter.type, equals('GCounter'));
        expect(counter.value, equals(0));
        expect(counter.isEmpty, isTrue);
      });

      test('should increment counter correctly', () {
        final counter = GCounter('test-counter');
        counter.increment('node1', 5);
        expect(counter.value, equals(5));
        expect(counter.getCounterFor('node1'), equals(5));
        expect(counter.getCounterFor('node2'), equals(0));
      });

      test('should merge states correctly', () {
        final counter1 = GCounter('test-counter');
        final counter2 = GCounter('test-counter');

        counter1.increment('node1', 3);
        counter2.increment('node2', 4);

        counter1.mergeState(counter2.getState());

        expect(counter1.value, equals(7));
        expect(counter1.getCounterFor('node1'), equals(3));
        expect(counter1.getCounterFor('node2'), equals(4));
      });

      test('should handle operations through CRDT interface', () {
        final counter = GCounter('test-counter');
        final operation = CRDTOperation(
          crdtId: 'test-counter',
          operation: 'increment',
          data: {'amount': 10},
          nodeId: 'node1',
          timestamp: DateTime.now(),
        );

        counter.applyOperation(operation);
        expect(counter.value, equals(10));
      });

      test('should serialize and deserialize state', () {
        final counter1 = GCounter('test-counter');
        counter1.increment('node1', 5);
        counter1.increment('node2', 3);

        final state = counter1.getState();
        final counter2 = GCounter.fromState(state);

        expect(counter2.id, equals('test-counter'));
        expect(counter2.value, equals(8));
        expect(counter2.getCounterFor('node1'), equals(5));
        expect(counter2.getCounterFor('node2'), equals(3));
      });

      test('should reject negative increments', () {
        final counter = GCounter('test-counter');
        expect(() => counter.increment('node1', -1), throwsArgumentError);
      });

      test('should handle concurrent updates correctly', () {
        final counter1 = GCounter('test-counter');
        final counter2 = GCounter('test-counter');

        // Both nodes increment the same counter for the same node
        counter1.increment('node1', 5);
        counter2.increment('node1', 3); // This should be ignored after merge

        // Merge should take the maximum value
        counter1.mergeState(counter2.getState());
        expect(counter1.getCounterFor('node1'), equals(5));
        expect(counter1.value, equals(5));
      });
    });

    group('PNCounter', () {
      test('should create counter with initial value zero', () {
        final counter = PNCounter('test-pn-counter');
        expect(counter.id, equals('test-pn-counter'));
        expect(counter.type, equals('PNCounter'));
        expect(counter.value, equals(0));
        expect(counter.totalPositive, equals(0));
        expect(counter.totalNegative, equals(0));
      });

      test('should increment and decrement correctly', () {
        final counter = PNCounter('test-pn-counter');
        counter.increment('node1', 10);
        counter.decrement('node1', 3);

        expect(counter.value, equals(7));
        expect(counter.getPositiveCounterFor('node1'), equals(10));
        expect(counter.getNegativeCounterFor('node1'), equals(3));
        expect(counter.getNetCounterFor('node1'), equals(7));
      });

      test('should merge states correctly', () {
        final counter1 = PNCounter('test-pn-counter');
        final counter2 = PNCounter('test-pn-counter');

        counter1.increment('node1', 10);
        counter1.decrement('node1', 2);

        counter2.increment('node2', 5);
        counter2.decrement('node2', 8);

        counter1.mergeState(counter2.getState());

        expect(counter1.value, equals(5)); // (10-2) + (5-8) = 8 + (-3) = 5
        expect(counter1.totalPositive, equals(15));
        expect(counter1.totalNegative, equals(10));
      });

      test('should handle operations through CRDT interface', () {
        final counter = PNCounter('test-pn-counter');

        final incOp = CRDTOperation(
          crdtId: 'test-pn-counter',
          operation: 'increment',
          data: {'amount': 15},
          nodeId: 'node1',
          timestamp: DateTime.now(),
        );

        final decOp = CRDTOperation(
          crdtId: 'test-pn-counter',
          operation: 'decrement',
          data: {'amount': 5},
          nodeId: 'node1',
          timestamp: DateTime.now(),
        );

        counter.applyOperation(incOp);
        counter.applyOperation(decOp);

        expect(counter.value, equals(10));
      });
    });

    group('GSet', () {
      test('should create empty set', () {
        final set = GSet<String>('test-set');
        expect(set.id, equals('test-set'));
        expect(set.type, equals('GSet'));
        expect(set.value, isEmpty);
        expect(set.size, equals(0));
        expect(set.isEmpty, isTrue);
      });

      test('should add elements correctly', () {
        final set = GSet<String>('test-set');
        set.add('apple');
        set.add('banana');

        expect(set.size, equals(2));
        expect(set.contains('apple'), isTrue);
        expect(set.contains('banana'), isTrue);
        expect(set.contains('cherry'), isFalse);
      });

      test('should handle duplicate additions', () {
        final set = GSet<String>('test-set');
        expect(set.add('apple'), isTrue); // First addition
        expect(set.add('apple'), isFalse); // Duplicate addition
        expect(set.size, equals(1));
      });

      test('should merge sets correctly', () {
        final set1 = GSet<String>('test-set');
        final set2 = GSet<String>('test-set');

        set1.add('apple');
        set1.add('banana');

        set2.add('banana');
        set2.add('cherry');

        set1.mergeState(set2.getState());

        expect(set1.size, equals(3));
        expect(set1.value, containsAll(['apple', 'banana', 'cherry']));
      });

      test('should handle operations through CRDT interface', () {
        final set = GSet<String>('test-set');
        final operation = CRDTOperation(
          crdtId: 'test-set',
          operation: 'add',
          data: {'element': 'test-element'},
          nodeId: 'node1',
          timestamp: DateTime.now(),
        );

        set.applyOperation(operation);
        expect(set.contains('test-element'), isTrue);
      });

      test('should support set operations', () {
        final set1 = GSet<String>('test-set');
        final set2 = <String>{'banana', 'cherry', 'date'};

        set1.add('apple');
        set1.add('banana');

        expect(
          set1.union(set2),
          containsAll(['apple', 'banana', 'cherry', 'date']),
        );
        expect(set1.intersection(set2), equals({'banana'}));
        expect(set1.difference(set2), equals({'apple'}));
        expect(set1.isSubsetOf(set2), isFalse);
        expect(set1.isSupersetOf({'banana'}), isTrue);
      });
    });
  });

  group('CRDT Storage', () {
    test('should save and load CRDTs', () async {
      final store = MemoryCRDTStore();
      final counter = GCounter('test-counter');
      counter.increment('node1', 5);

      await store.saveCRDT(counter);
      expect(await store.hasCRDT('test-counter'), isTrue);

      final loadedState = await store.loadCRDTState('test-counter');
      expect(loadedState, isNotNull);
      expect(loadedState!['type'], equals('GCounter'));
      expect(loadedState['id'], equals('test-counter'));

      final restoredCounter = GCounter.fromState(loadedState);
      expect(restoredCounter.value, equals(5));

      await store.close();
    });

    test('should handle non-existent CRDTs', () async {
      final store = MemoryCRDTStore();

      expect(await store.hasCRDT('non-existent'), isFalse);
      expect(await store.loadCRDTState('non-existent'), isNull);

      await store.close();
    });

    test('should provide statistics', () async {
      final store = MemoryCRDTStore();
      final counter1 = GCounter('counter1');
      final counter2 = GCounter('counter2');

      await store.saveCRDT(counter1);
      await store.saveCRDT(counter2);

      final stats = await store.getStats();
      expect(stats.totalCRDTs, equals(2));

      await store.close();
    });
  });

  group('CRDT Manager Integration', () {
    late Map<String, MockTransport> network;

    setUp(() {
      network = <String, MockTransport>{};
    });

    tearDown(() async {
      for (final transport in network.values) {
        await transport.shutdown();
      }
    });

    test('should enable CRDT support on GossipNode', () async {
      final gossipNode = GossipNode(
        config: GossipConfig(nodeId: 'test-node'),
        eventStore: MemoryEventStore(),
        transport: MockTransport('test-node', network),
      );

      await gossipNode.start();

      final crdtManager = await gossipNode.enableCRDTSupport();
      expect(crdtManager, isNotNull);
      expect(crdtManager.isInitialized, isTrue);

      final counter = GCounter('test-counter');
      await crdtManager.register(counter);
      expect(crdtManager.getCRDT<GCounter>('test-counter'), isNotNull);

      await crdtManager.close();
      await gossipNode.stop();
    });

    test('should synchronize CRDTs between nodes', () async {
      // Create two nodes
      final nodeA = GossipNode(
        config: GossipConfig(nodeId: 'nodeA'),
        eventStore: MemoryEventStore(),
        transport: MockTransport('nodeA', network),
      );

      final nodeB = GossipNode(
        config: GossipConfig(nodeId: 'nodeB'),
        eventStore: MemoryEventStore(),
        transport: MockTransport('nodeB', network),
      );

      await nodeA.start();
      await nodeB.start();

      // Enable CRDT support
      final crdtManagerA = await nodeA.enableCRDTSupport();
      final crdtManagerB = await nodeB.enableCRDTSupport();

      // Add peers
      nodeA.addPeer(GossipPeer(id: 'nodeB', address: 'mock://nodeB'));
      nodeB.addPeer(GossipPeer(id: 'nodeA', address: 'mock://nodeA'));

      // Register counters
      final counterA = GCounter('shared-counter');
      final counterB = GCounter('shared-counter');

      await crdtManagerA.register(counterA);
      await crdtManagerB.register(counterB);

      // Perform operations
      await crdtManagerA.performOperation('shared-counter', 'increment', {
        'amount': 5,
      });
      await crdtManagerB.performOperation('shared-counter', 'increment', {
        'amount': 3,
      });

      // Allow time for gossip synchronization
      await Future.delayed(Duration(milliseconds: 100));

      // Trigger manual gossip
      await nodeA.gossip();
      await Future.delayed(Duration(milliseconds: 100));

      // Both counters should have the same total value
      expect(counterA.value, equals(counterB.value));
      expect(counterA.value, equals(8)); // 5 + 3

      await crdtManagerA.close();
      await crdtManagerB.close();
      await nodeA.stop();
      await nodeB.stop();
    });

    test('should emit events for CRDT operations', () async {
      final gossipNode = GossipNode(
        config: GossipConfig(nodeId: 'test-node'),
        eventStore: MemoryEventStore(),
        transport: MockTransport('test-node', network),
      );

      await gossipNode.start();
      final crdtManager = await gossipNode.enableCRDTSupport();

      final counter = GCounter('test-counter');
      await crdtManager.register(counter);

      // Listen to events
      final updateEvents = <CRDTUpdateEvent>[];
      final operationEvents = <CRDTOperationEvent>[];

      final updateSub = crdtManager.onUpdate.listen(updateEvents.add);
      final operationSub = crdtManager.onOperation.listen(operationEvents.add);

      // Perform operation
      await crdtManager.performOperation('test-counter', 'increment', {
        'amount': 5,
      });

      await Future.delayed(Duration(milliseconds: 50));

      expect(updateEvents, hasLength(greaterThan(0)));
      expect(operationEvents, hasLength(1));
      expect(operationEvents.first.operation.operation, equals('increment'));
      expect(operationEvents.first.source, equals(CRDTOperationSource.local));

      await updateSub.cancel();
      await operationSub.cancel();
      await crdtManager.close();
      await gossipNode.stop();
    });
  });

  group('CRDTEnabledGossipNode', () {
    late Map<String, MockTransport> network;

    setUp(() {
      network = <String, MockTransport>{};
    });

    tearDown(() async {
      for (final transport in network.values) {
        await transport.shutdown();
      }
    });

    test('should provide unified interface', () async {
      final gossipNode = GossipNode(
        config: GossipConfig(nodeId: 'test-node'),
        eventStore: MemoryEventStore(),
        transport: MockTransport('test-node', network),
      );

      final crdtNode = await gossipNode.withCRDTSupport();
      await crdtNode.start();

      // Test gossip functionality
      await crdtNode.createEvent({'type': 'test', 'data': 'hello'});

      // Test CRDT functionality
      final counter = GCounter('test-counter');
      await crdtNode.registerCRDT(counter);
      await crdtNode.performCRDTOperation('test-counter', 'increment', {
        'amount': 1,
      });

      expect(crdtNode.getCRDT<GCounter>('test-counter')?.value, equals(1));

      await crdtNode.close();
    });
  });

  group('CRDT Operations Validation', () {
    test('should validate CRDT operation parameters', () {
      final counter = GCounter('test-counter');

      expect(
        () => counter.applyOperation(
          CRDTOperation(
            crdtId: 'test-counter',
            operation: 'unknown-operation',
            data: {},
            nodeId: 'node1',
            timestamp: DateTime.now(),
          ),
        ),
        throwsA(isA<CRDTException>()),
      );
    });

    test('should validate state merging', () {
      final counter = GCounter('test-counter');

      expect(
        () => counter.mergeState({
          'type': 'WrongType',
          'id': 'test-counter',
          'counters': {},
        }),
        throwsA(isA<CRDTException>()),
      );

      expect(
        () => counter.mergeState({
          'type': 'GCounter',
          'id': 'wrong-id',
          'counters': {},
        }),
        throwsA(isA<CRDTException>()),
      );
    });

    test('should validate CRDT construction from state', () {
      expect(
        () => GCounter.fromState({
          'type': 'WrongType',
          'id': 'test-counter',
          'counters': {},
        }),
        throwsA(isA<CRDTException>()),
      );

      expect(
        () => GCounter.fromState({
          'type': 'GCounter',
          'id': 'test-counter',
          'counters': {'node1': -5}, // Invalid negative value
        }),
        throwsA(isA<CRDTException>()),
      );
    });
  });
}
