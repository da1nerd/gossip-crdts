/// Extension to add CRDT capabilities to existing SimpleGossipNode instances.
///
/// This module provides a convenient extension method that allows any existing
/// SimpleGossipNode to be enhanced with CRDT functionality without modifying the
/// original gossip protocol library.
library;

import 'package:gossip/gossip.dart';

import '../crdt_manager.dart';
import '../crdt_store.dart';
import '../crdts/crdt.dart';

/// Extension to add CRDT capabilities to existing SimpleGossipNode instances.
///
/// This extension provides a seamless way to add CRDT support to any SimpleGossipNode
/// without requiring changes to the core gossip protocol library. Since SimpleGossipNode
/// doesn't have the full event infrastructure of GossipNode, this extension provides
/// a simplified CRDT management approach.
///
/// ## Usage
///
/// ```dart
/// import 'package:gossip/gossip.dart';
/// import 'package:gossip_crdts/gossip_crdts.dart';
///
/// final node = SimpleGossipNode(
///   nodeId: 'node1',
///   transport: MySimpleTransport(),
///   eventStore: MemoryEventStore(),
/// );
///
/// await node.start();
///
/// // Enable CRDT support
/// final crdtManager = await node.enableCRDTSupport();
///
/// // Register and use CRDTs
/// final counter = GCounter('shared-counter');
/// await crdtManager.register(counter);
/// await crdtManager.performOperation('shared-counter', 'increment', {'amount': 5});
/// ```
extension SimpleGossipNodeCRDTExtension on SimpleGossipNode {
  /// Enable CRDT support for this simple gossip node.
  ///
  /// This method sets up CRDT functionality by creating a SimpleCRDTManager that
  /// coordinates with the simple gossip protocol for operation broadcasting and
  /// state synchronization.
  ///
  /// Parameters:
  /// - [crdtStore]: Optional custom storage backend. If not provided,
  ///   uses an in-memory store (suitable for testing/development)
  ///
  /// Returns a [SimpleCRDTManager] that can be used to register and manage CRDTs.
  ///
  /// Throws [CRDTException] if CRDT support cannot be enabled.
  ///
  /// ## Example
  ///
  /// ```dart
  /// // Basic usage with default memory storage
  /// final crdtManager = await node.enableCRDTSupport();
  ///
  /// // With custom storage
  /// final customStore = MyCRDTStore();
  /// final crdtManager = await node.enableCRDTSupport(crdtStore: customStore);
  /// ```
  Future<SimpleCRDTManager> enableCRDTSupport({CRDTStore? crdtStore}) async {
    final store = crdtStore ?? MemoryCRDTStore();
    final manager = SimpleCRDTManager(this, store);

    await manager.initialize();

    return manager;
  }

  /// Create a CRDT-enabled version of this simple gossip node.
  ///
  /// This is an alternative to [enableCRDTSupport] that returns a wrapper
  /// object that combines the original simple gossip node with CRDT management
  /// capabilities.
  ///
  /// Parameters:
  /// - [crdtStore]: Optional custom storage backend
  ///
  /// Returns a [CRDTEnabledSimpleGossipNode] that provides both gossip and CRDT
  /// functionality in a single interface.
  ///
  /// ## Example
  ///
  /// ```dart
  /// final crdtNode = await node.withCRDTSupport();
  ///
  /// // Use simple gossip functionality
  /// await crdtNode.createEvent({'type': 'message', 'content': 'hello'});
  ///
  /// // Use CRDT functionality
  /// final counter = GCounter('my-counter');
  /// await crdtNode.registerCRDT(counter);
  /// await crdtNode.performCRDTOperation('my-counter', 'increment', {'amount': 1});
  /// ```
  Future<CRDTEnabledSimpleGossipNode> withCRDTSupport({
    CRDTStore? crdtStore,
  }) async {
    final manager = await enableCRDTSupport(crdtStore: crdtStore);
    return CRDTEnabledSimpleGossipNode._(this, manager);
  }
}

/// Simplified CRDT manager for SimpleGossipNode.
///
/// This manager provides CRDT functionality for SimpleGossipNode instances,
/// handling operation broadcasting and state synchronization through the
/// simple event system.
class SimpleCRDTManager {
  final SimpleGossipNode _gossipNode;
  final CRDTStore _store;
  final Map<String, CRDT> _crdts = {};

  // Stream controllers for CRDT events
  final StreamController<CRDTUpdateEvent> _updateController =
      StreamController<CRDTUpdateEvent>.broadcast();
  final StreamController<CRDTOperationEvent> _operationController =
      StreamController<CRDTOperationEvent>.broadcast();

  bool _isInitialized = false;
  bool _isClosed = false;
  StreamSubscription? _eventSubscription;

  /// Creates a new simple CRDT manager.
  SimpleCRDTManager(this._gossipNode, this._store);

  /// Initialize the CRDT manager.
  Future<void> initialize() async {
    if (_isInitialized) return;

    _checkNotClosed();

    // Listen to incoming events for CRDT operations
    _eventSubscription = _gossipNode.onEventReceived.listen((event) {
      _handleSimpleGossipEvent(event);
    });

    // Load existing CRDTs from storage
    await _loadCRDTsFromStorage();

    _isInitialized = true;
  }

  /// Register a CRDT with this manager.
  Future<void> register<T extends CRDT>(T crdt) async {
    _checkInitialized();

    if (_crdts.containsKey(crdt.id)) {
      throw CRDTException(
        'CRDT with ID ${crdt.id} is already registered',
        crdtId: crdt.id,
      );
    }

    _crdts[crdt.id] = crdt;
    await _store.saveCRDT(crdt);

    _updateController.add(
      CRDTUpdateEvent(
        crdtId: crdt.id,
        type: CRDTUpdateType.registered,
        timestamp: DateTime.now(),
      ),
    );
  }

  /// Unregister a CRDT from this manager.
  Future<bool> unregister(String crdtId) async {
    _checkInitialized();

    final crdt = _crdts.remove(crdtId);
    if (crdt != null) {
      _updateController.add(
        CRDTUpdateEvent(
          crdtId: crdtId,
          type: CRDTUpdateType.unregistered,
          timestamp: DateTime.now(),
        ),
      );
      return true;
    }
    return false;
  }

  /// Get a CRDT by its ID.
  T? getCRDT<T extends CRDT>(String crdtId) {
    _checkInitialized();
    return _crdts[crdtId] as T?;
  }

  /// Get all registered CRDT IDs.
  List<String> getCRDTIds() {
    _checkInitialized();
    return List.unmodifiable(_crdts.keys);
  }

  /// Get all registered CRDTs.
  List<CRDT> getAllCRDTs() {
    _checkInitialized();
    return List.unmodifiable(_crdts.values);
  }

  /// Perform a CRDT operation.
  Future<void> performOperation(
    String crdtId,
    String operation,
    Map<String, dynamic> data,
  ) async {
    _checkInitialized();

    final crdt = _crdts[crdtId];
    if (crdt == null) {
      throw CRDTException('CRDT with ID $crdtId not found', crdtId: crdtId);
    }

    final crdtOp = CRDTOperation.withId(
      crdtId: crdtId,
      operation: operation,
      data: data,
      nodeId: _gossipNode.nodeId,
      timestamp: DateTime.now(),
    );

    try {
      // Apply locally first
      crdt.applyOperation(crdtOp);
      await _store.saveCRDT(crdt);

      // Broadcast to other nodes through simple gossip
      await _gossipNode.createEvent({
        'type': 'crdt_operation',
        'operation': crdtOp.toJson(),
      });

      // Notify listeners
      _operationController.add(
        CRDTOperationEvent(
          operation: crdtOp,
          source: CRDTOperationSource.local,
          timestamp: DateTime.now(),
        ),
      );

      _updateController.add(
        CRDTUpdateEvent(
          crdtId: crdtId,
          type: CRDTUpdateType.operationApplied,
          timestamp: DateTime.now(),
          operation: crdtOp,
        ),
      );
    } catch (e, stackTrace) {
      throw CRDTException(
        'Failed to perform operation $operation on CRDT $crdtId: $e',
        crdtId: crdtId,
        operation: crdtOp,
        cause: e,
      );
    }
  }

  /// Synchronize all CRDT states.
  Future<void> syncAll() async {
    _checkInitialized();

    final states = <String, Map<String, dynamic>>{};
    for (final crdt in _crdts.values) {
      states[crdt.id] = crdt.getState();
    }

    await _gossipNode.createEvent({'type': 'crdt_sync', 'states': states});
  }

  /// Stream of CRDT update events.
  Stream<CRDTUpdateEvent> get onUpdate => _updateController.stream;

  /// Stream of CRDT operation events.
  Stream<CRDTOperationEvent> get onOperation => _operationController.stream;

  /// Close the manager and release resources.
  Future<void> close() async {
    if (_isClosed) return;

    _isClosed = true;
    _isInitialized = false;

    await _eventSubscription?.cancel();
    await _updateController.close();
    await _operationController.close();
    await _store.close();
  }

  /// Handle incoming events from SimpleGossipNode.
  void _handleSimpleGossipEvent(Event event) {
    final eventType = event.payload['type'] as String?;

    switch (eventType) {
      case 'crdt_operation':
        _handleCRDTOperation(event);
        break;
      case 'crdt_sync':
        _handleCRDTSync(event);
        break;
    }
  }

  /// Handle a CRDT operation from another node.
  Future<void> _handleCRDTOperation(Event event) async {
    try {
      final operationData = event.payload['operation'] as Map<String, dynamic>;
      final operation = CRDTOperation.fromJson(operationData);

      final crdt = _crdts[operation.crdtId];
      if (crdt != null) {
        crdt.applyOperation(operation);
        await _store.saveCRDT(crdt);

        _operationController.add(
          CRDTOperationEvent(
            operation: operation,
            source: CRDTOperationSource.remote,
            timestamp: DateTime.now(),
          ),
        );

        _updateController.add(
          CRDTUpdateEvent(
            crdtId: operation.crdtId,
            type: CRDTUpdateType.operationApplied,
            timestamp: DateTime.now(),
            operation: operation,
          ),
        );
      }
    } catch (e) {
      // Log error but don't propagate - gossip should be resilient
    }
  }

  /// Handle CRDT state synchronization from another node.
  Future<void> _handleCRDTSync(Event event) async {
    try {
      final states = event.payload['states'] as Map<String, dynamic>;
      await _mergeCRDTStates(states);
    } catch (e) {
      // Log error but don't propagate
    }
  }

  /// Merge CRDT states from another node.
  Future<void> _mergeCRDTStates(Map<String, dynamic> states) async {
    for (final entry in states.entries) {
      final crdtId = entry.key;
      final state = entry.value as Map<String, dynamic>;

      final crdt = _crdts[crdtId];
      if (crdt != null) {
        crdt.mergeState(state);
        await _store.saveCRDT(crdt);
      }
    }
  }

  /// Load previously saved CRDTs from storage.
  Future<void> _loadCRDTsFromStorage() async {
    try {
      final crdtIds = await _store.getAllCRDTIds();
      // Note: This is a basic implementation. In practice, you'd need
      // a registry of CRDT factories to recreate CRDTs from stored state.
    } catch (e) {
      // Log warning but continue
    }
  }

  void _checkInitialized() {
    if (!_isInitialized) {
      throw CRDTException('Simple CRDT manager has not been initialized');
    }
    _checkNotClosed();
  }

  void _checkNotClosed() {
    if (_isClosed) {
      throw CRDTException('Simple CRDT manager has been closed');
    }
  }

  /// Get statistics about managed CRDTs.
  SimpleCRDTManagerStats getStats() {
    return SimpleCRDTManagerStats(
      totalCRDTs: _crdts.length,
      crdtTypes: _crdts.values.map((crdt) => crdt.type).toSet().toList(),
      isInitialized: _isInitialized,
      isClosed: _isClosed,
    );
  }
}

/// A wrapper that combines SimpleGossipNode and SimpleCRDTManager functionality.
class CRDTEnabledSimpleGossipNode {
  final SimpleGossipNode _gossipNode;
  final SimpleCRDTManager _crdtManager;

  CRDTEnabledSimpleGossipNode._(this._gossipNode, this._crdtManager);

  /// Access to the underlying simple gossip node.
  SimpleGossipNode get gossipNode => _gossipNode;

  /// Access to the simple CRDT manager.
  SimpleCRDTManager get crdtManager => _crdtManager;

  // Delegate common simple gossip operations

  /// Create an event and broadcast it.
  Future<Event> createEvent(Map<String, dynamic> payload) =>
      _gossipNode.createEvent(payload);

  /// Start the gossip node.
  Future<void> start() => _gossipNode.start();

  /// Stop the gossip node.
  Future<void> stop() => _gossipNode.stop();

  /// Get the node ID.
  String get nodeId => _gossipNode.nodeId;

  /// Get the current vector clock.
  VectorClock get vectorClock => _gossipNode.vectorClock;

  /// Stream of events created by this node.
  Stream<Event> get onEventCreated => _gossipNode.onEventCreated;

  /// Stream of events received from other nodes.
  Stream<Event> get onEventReceived => _gossipNode.onEventReceived;

  /// Stream of peer join events.
  Stream<String> get onPeerJoined => _gossipNode.onPeerJoined;

  /// Stream of peer leave events.
  Stream<String> get onPeerLeft => _gossipNode.onPeerLeft;

  /// Get list of connected peers.
  List<String> get connectedPeers => _gossipNode.connectedPeers;

  // Delegate CRDT operations with convenience methods

  /// Register a CRDT with the manager.
  Future<void> registerCRDT<T extends CRDT>(T crdt) =>
      _crdtManager.register(crdt);

  /// Unregister a CRDT from the manager.
  Future<bool> unregisterCRDT(String crdtId) => _crdtManager.unregister(crdtId);

  /// Get a CRDT by its ID.
  T? getCRDT<T extends CRDT>(String crdtId) => _crdtManager.getCRDT<T>(crdtId);

  /// Get all registered CRDT IDs.
  List<String> getCRDTIds() => _crdtManager.getCRDTIds();

  /// Get all registered CRDTs.
  List<CRDT> getAllCRDTs() => _crdtManager.getAllCRDTs();

  /// Perform a CRDT operation.
  Future<void> performCRDTOperation(
    String crdtId,
    String operation,
    Map<String, dynamic> data,
  ) => _crdtManager.performOperation(crdtId, operation, data);

  /// Synchronize all CRDT states.
  Future<void> syncAllCRDTs() => _crdtManager.syncAll();

  /// Stream of CRDT update events.
  Stream<CRDTUpdateEvent> get onCRDTUpdate => _crdtManager.onUpdate;

  /// Stream of CRDT operation events.
  Stream<CRDTOperationEvent> get onCRDTOperation => _crdtManager.onOperation;

  /// Get statistics about the CRDT manager.
  SimpleCRDTManagerStats getCRDTStats() => _crdtManager.getStats();

  /// Close both the gossip node and CRDT manager.
  Future<void> close() async {
    await _crdtManager.close();
    await _gossipNode.dispose();
  }

  @override
  String toString() =>
      'CRDTEnabledSimpleGossipNode(nodeId: $nodeId, '
      'crdtCount: ${getCRDTIds().length})';
}

/// Statistics about a simple CRDT manager.
class SimpleCRDTManagerStats {
  final int totalCRDTs;
  final List<String> crdtTypes;
  final bool isInitialized;
  final bool isClosed;

  const SimpleCRDTManagerStats({
    required this.totalCRDTs,
    required this.crdtTypes,
    required this.isInitialized,
    required this.isClosed,
  });

  @override
  String toString() =>
      'SimpleCRDTManagerStats(totalCRDTs: $totalCRDTs, '
      'crdtTypes: $crdtTypes, isInitialized: $isInitialized, '
      'isClosed: $isClosed)';
}
