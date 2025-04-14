use crate::bindings::{self, *};
use num_enum::TryFromPrimitive;
use std::{convert::TryFrom, error::Error, ffi::CStr, fmt};

/// Unknown partition constant
pub const RD_KAFKA_PARTITION_UA: i32 = -1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ::num_enum::TryFromPrimitive)]
#[repr(u32)]
pub enum RDKafkaType {
    Producer = rd_kafka_type_t::RD_KAFKA_PRODUCER as u32,
    Consumer = rd_kafka_type_t::RD_KAFKA_CONSUMER as u32,
}

impl From<RDKafkaType> for rd_kafka_type_t {
    fn from(value: RDKafkaType) -> Self {
        match value {
            RDKafkaType::Producer => rd_kafka_type_t::RD_KAFKA_PRODUCER,
            RDKafkaType::Consumer => rd_kafka_type_t::RD_KAFKA_CONSUMER,
        }
    }
}

impl From<rd_kafka_type_t> for RDKafkaType {
    fn from(value: rd_kafka_type_t) -> Self {
        match value {
            rd_kafka_type_t::RD_KAFKA_PRODUCER => Self::Producer,
            rd_kafka_type_t::RD_KAFKA_CONSUMER => Self::Consumer,
        }
    }
}

/// Native rdkafka error code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ::num_enum::TryFromPrimitive)]
#[repr(i32)]
#[non_exhaustive]
pub enum RDKafkaErrorCode {
    #[doc(hidden)]
    Begin = -200,
    /// Received message is incorrect.
    BadMessage = -199,
    /// Bad/unknown compression.
    BadCompression = -198,
    /// Broker is going away.
    BrokerDestroy = -197,
    /// Generic failure.
    Fail = -196,
    /// Broker transport failure.
    BrokerTransportFailure = -195,
    /// Critical system resource.
    CriticalSystemResource = -194,
    /// Failed to resolve broker.
    Resolve = -193,
    /// Produced message timed out.
    MessageTimedOut = -192,
    /// Reached the end of the topic+partition queue on the broker. Not really an error.
    PartitionEOF = -191,
    /// Permanent: Partition does not exist in cluster.
    UnknownPartition = -190,
    /// File or filesystem error.
    FileSystem = -189,
    /// Permanent: Topic does not exist in cluster.
    UnknownTopic = -188,
    /// All broker connections are down.
    AllBrokersDown = -187,
    /// Invalid argument, or invalid configuration.
    InvalidArgument = -186,
    /// Operation timed out.
    OperationTimedOut = -185,
    /// Queue is full.
    QueueFull = -184,
    /// ISR count < required.acks.
    ISRInsufficient = -183,
    /// Broker node update.
    NodeUpdate = -182,
    /// SSL error.
    SSL = -181,
    /// Waiting for coordinator to become available.
    WaitingForCoordinator = -180,
    /// Unknown client group.
    UnknownGroup = -179,
    /// Operation in progress.
    InProgress = -178,
    /// Previous operation in progress, wait for it to finish.
    PreviousInProgress = -177,
    /// This operation would interfere with an existing subscription.
    ExistingSubscription = -176,
    /// Assigned partitions (rebalance_cb).
    AssignPartitions = -175,
    /// Revoked partitions (rebalance_cb).
    RevokePartitions = -174,
    /// Conflicting use.
    Conflict = -173,
    /// Wrong state.
    State = -172,
    /// Unknown protocol.
    UnknownProtocol = -171,
    /// Not implemented.
    NotImplemented = -170,
    /// Authentication failure.
    Authentication = -169,
    /// No stored offset.
    NoOffset = -168,
    /// Outdated.
    Outdated = -167,
    /// Timed out in queue.
    TimedOutQueue = -166,
    /// Feature not supported by broker.
    UnsupportedFeature = -165,
    /// Awaiting cache update.
    WaitCache = -164,
    /// Operation interrupted (e.g., due to yield).
    Interrupted = -163,
    /// Key serialization error.
    KeySerialization = -162,
    /// Value serialization error.
    ValueSerialization = -161,
    /// Key deserialization error.
    KeyDeserialization = -160,
    /// Value deserialization error.
    ValueDeserialization = -159,
    /// Partial response.
    Partial = -158,
    /// Modification attempted on read-only object.
    ReadOnly = -157,
    /// No such entry or item not found.
    NoEnt = -156,
    /// Read underflow.
    Underflow = -155,
    /// Invalid type.
    InvalidType = -154,
    /// Retry operation.
    Retry = -153,
    /// Purged in queue.
    PurgeQueue = -152,
    /// Purged in flight.
    PurgeInflight = -151,
    /// Fatal error: see rd_kafka_fatal_error().
    Fatal = -150,
    /// Inconsistent state.
    Inconsistent = -149,
    /// Gap-less ordering would not be guaranteed if proceeding.
    GaplessGuarantee = -148,
    /// Maximum poll interval exceeded.
    PollExceeded = -147,
    /// Unknown broker.
    UnknownBroker = -146,
    /// Functionality not configured.
    NotConfigured = -145,
    /// Instance has been fenced.
    Fenced = -144,
    /// Application generated error.
    Application = -143,
    /// Assignment lost.
    AssignmentLost = -142,
    /// No operation performed.
    Noop = -141,
    /// No offset to automatically reset to.
    AutoOffsetReset = -140,
    /// Partition log truncation detected
    LogTruncation = -139,
    /// A different record in the batch was invalid and this message failed persisting.
    InvalidDifferentRecord = -138,
    #[doc(hidden)]
    End = -100,
    /// Unknown broker error.
    Unknown = -1,
    /// Success.
    NoError = 0,
    /// Offset out of range.
    OffsetOutOfRange = 1,
    /// Invalid message.
    InvalidMessage = 2,
    /// Unknown topic or partition.
    UnknownTopicOrPartition = 3,
    /// Invalid message size.
    InvalidMessageSize = 4,
    /// Leader not available.
    LeaderNotAvailable = 5,
    /// Not leader for partition.
    NotLeaderForPartition = 6,
    /// Request timed out.
    RequestTimedOut = 7,
    /// Broker not available.
    BrokerNotAvailable = 8,
    /// Replica not available.
    ReplicaNotAvailable = 9,
    /// Message size too large.
    MessageSizeTooLarge = 10,
    /// Stale controller epoch code.
    StaleControllerEpoch = 11,
    /// Offset metadata string too large.
    OffsetMetadataTooLarge = 12,
    /// Broker disconnected before response received.
    NetworkException = 13,
    /// Coordinator load in progress.
    CoordinatorLoadInProgress = 14,
    /// Coordinator not available.
    CoordinatorNotAvailable = 15,
    /// Not coordinator.
    NotCoordinator = 16,
    /// Invalid topic.
    InvalidTopic = 17,
    /// Message batch larger than configured server segment size.
    MessageBatchTooLarge = 18,
    /// Not enough in-sync replicas.
    NotEnoughReplicas = 19,
    /// Message(s) written to insufficient number of in-sync replicas.
    NotEnoughReplicasAfterAppend = 20,
    /// Invalid required acks value.
    InvalidRequiredAcks = 21,
    /// Specified group generation id is not valid.
    IllegalGeneration = 22,
    /// Inconsistent group protocol.
    InconsistentGroupProtocol = 23,
    /// Invalid group.id.
    InvalidGroupId = 24,
    /// Unknown member.
    UnknownMemberId = 25,
    /// Invalid session timeout.
    InvalidSessionTimeout = 26,
    /// Group rebalance in progress.
    RebalanceInProgress = 27,
    /// Commit offset data size is not valid.
    InvalidCommitOffsetSize = 28,
    /// Topic authorization failed.
    TopicAuthorizationFailed = 29,
    /// Group authorization failed.
    GroupAuthorizationFailed = 30,
    /// Cluster authorization failed.
    ClusterAuthorizationFailed = 31,
    /// Invalid timestamp.
    InvalidTimestamp = 32,
    /// Unsupported SASL mechanism.
    UnsupportedSASLMechanism = 33,
    /// Illegal SASL state.
    IllegalSASLState = 34,
    /// Unsupported version.
    UnsupportedVersion = 35,
    /// Topic already exists.
    TopicAlreadyExists = 36,
    /// Invalid number of partitions.
    InvalidPartitions = 37,
    /// Invalid replication factor.
    InvalidReplicationFactor = 38,
    /// Invalid replica assignment.
    InvalidReplicaAssignment = 39,
    /// Invalid config.
    InvalidConfig = 40,
    /// Not controller for cluster.
    NotController = 41,
    /// Invalid request.
    InvalidRequest = 42,
    /// Message format on broker does not support request.
    UnsupportedForMessageFormat = 43,
    /// Policy violation.
    PolicyViolation = 44,
    /// Broker received an out of order sequence number.
    OutOfOrderSequenceNumber = 45,
    /// Broker received a duplicate sequence number.
    DuplicateSequenceNumber = 46,
    /// Producer attempted an operation with an old epoch.
    InvalidProducerEpoch = 47,
    /// Producer attempted a transactional operation in an invalid state.
    InvalidTransactionalState = 48,
    /// Producer attempted to use a producer id which is currently assigned to
    /// its transactional id.
    InvalidProducerIdMapping = 49,
    /// Transaction timeout is larger than the maxi value allowed by the
    /// broker's max.transaction.timeout.ms.
    InvalidTransactionTimeout = 50,
    /// Producer attempted to update a transaction while another concurrent
    /// operation on the same transaction was ongoing.
    ConcurrentTransactions = 51,
    /// Indicates that the transaction coordinator sending a WriteTxnMarker is
    /// no longer the current coordinator for a given producer.
    TransactionCoordinatorFenced = 52,
    /// Transactional Id authorization failed.
    TransactionalIdAuthorizationFailed = 53,
    /// Security features are disabled.
    SecurityDisabled = 54,
    /// Operation not attempted.
    OperationNotAttempted = 55,
    /// Disk error when trying to access log file on the disk.
    KafkaStorageError = 56,
    /// The user-specified log directory is not found in the broker config.
    LogDirNotFound = 57,
    /// SASL Authentication failed.
    SaslAuthenticationFailed = 58,
    /// Unknown Producer Id.
    UnknownProducerId = 59,
    /// Partition reassignment is in progress.
    ReassignmentInProgress = 60,
    /// Delegation Token feature is not enabled.
    DelegationTokenAuthDisabled = 61,
    /// Delegation Token is not found on server.
    DelegationTokenNotFound = 62,
    /// Specified Principal is not valid Owner/Renewer.
    DelegationTokenOwnerMismatch = 63,
    /// Delegation Token requests are not allowed on this connection.
    DelegationTokenRequestNotAllowed = 64,
    /// Delegation Token authorization failed.
    DelegationTokenAuthorizationFailed = 65,
    /// Delegation Token is expired.
    DelegationTokenExpired = 66,
    /// Supplied principalType is not supported.
    InvalidPrincipalType = 67,
    /// The group is not empty.
    NonEmptyGroup = 68,
    /// The group id does not exist.
    GroupIdNotFound = 69,
    /// The fetch session ID was not found.
    FetchSessionIdNotFound = 70,
    /// The fetch session epoch is invalid.
    InvalidFetchSessionEpoch = 71,
    /// No matching listener.
    ListenerNotFound = 72,
    /// Topic deletion is disabled.
    TopicDeletionDisabled = 73,
    /// Leader epoch is older than broker epoch.
    FencedLeaderEpoch = 74,
    /// Leader epoch is newer than broker epoch.
    UnknownLeaderEpoch = 75,
    /// Unsupported compression type.
    UnsupportedCompressionType = 76,
    /// Broker epoch has changed.
    StaleBrokerEpoch = 77,
    /// Leader high watermark is not caught up.
    OffsetNotAvailable = 78,
    /// Group member needs a valid member ID.
    MemberIdRequired = 79,
    /// Preferred leader was not available.
    PreferredLeaderNotAvailable = 80,
    /// Consumer group has reached maximum size.
    GroupMaxSizeReached = 81,
    /// Static consumer fenced by other consumer with same group.instance.id.
    FencedInstanceId = 82,
    /// Eligible partition leaders are not available.
    EligibleLeadersNotAvailable = 83,
    /// Leader election not needed for topic partition.
    ElectionNotNeeded = 84,
    /// No partition reassignment is in progress.
    NoReassignmentInProgress = 85,
    /// Deleting offsets of a topic while the consumer group is subscribed to
    /// it.
    GroupSubscribedToTopic = 86,
    /// Broker failed to validate record.
    InvalidRecord = 87,
    /// There are unstable offsets that need to be cleared.
    UnstableOffsetCommit = 88,
    /// Throttling quota has been exceeded.
    ThrottlingQuotaExceeded = 89,
    /// There is a newer producer with the same transactional ID which fences
    /// the current one.
    ProducerFenced = 90,
    /// Request illegally referred to resource that does not exist.
    ResourceNotFound = 91,
    /// Request illegally referred to the same resource twice.
    DuplicateResource = 92,
    /// Requested credential would not meet criteria for acceptability.
    UnacceptableCredential = 93,
    /// Either the sender or recipient of a voter-only request is not one of the
    /// expected voters.
    InconsistentVoterSet = 94,
    /// Invalid update version.
    InvalidUpdateVersion = 95,
    /// Unable to update finalized features due to server error.
    FeatureUpdateFailed = 96,
    /// Request principal deserialization failed during forwarding.
    PrincipalDeserializationFailure = 97,
    /// Unknown topic id
    UnknownTopicId = 100,
    /// The member epoch is fenced by the group coordinator
    FencedMemberEpoch = 110,
    /// The instance ID is still used by another member in the consumer group
    UnreleasedInstanceId = 111,
    /// The assignor or its version range is not supported by the consumer group
    UnsupportedAssignor = 112,
    /// The member epoch is stale
    StaleMemberEpoch = 113,
    /// Client sent a push telemetry request with an invalid or outdated subscription ID.
    UnknownSubscriptionId = 117,
    /// Client sent a push telemetry request larger than the maximum size the broker will accept.
    TelemetryTooLarge = 118,
    #[doc(hidden)]
    EndAll,
}

impl RDKafkaErrorCode {
    /// Returns native err name only (no description)
    pub fn name(&self) -> String {
        let cstr = unsafe { rd_kafka_err2name((*self).into()) };
        unsafe { CStr::from_ptr(cstr) }
            .to_string_lossy()
            .into_owned()
    }

    /// Returns an error if any
    pub fn error(self) -> Option<Self> {
        (!matches!(self, RDKafkaErrorCode::NoError)).then_some(self)
    }
}

impl From<RDKafkaErrorCode> for rd_kafka_resp_err_t {
    fn from(err: RDKafkaErrorCode) -> Self {
        // UNWRAP: seemless conversion
        Self::try_from(err as i32).unwrap()
    }
}

impl From<rd_kafka_resp_err_t> for RDKafkaErrorCode {
    fn from(err: rd_kafka_resp_err_t) -> RDKafkaErrorCode {
        // UNWRAP: seemless conversion
        Self::try_from(err as i32).unwrap()
    }
}

impl fmt::Display for RDKafkaErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let cstr = unsafe { rd_kafka_err2str((*self).into()) };
        let description = unsafe { CStr::from_ptr(cstr) }
            .to_string_lossy()
            .into_owned();

        write!(f, "{:?} ({})", self, description)
    }
}

impl Error for RDKafkaErrorCode {}

/// Errors for rdkafka configuration
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, TryFromPrimitive)]
#[repr(i32)]
pub enum RDKafkaConfErrorCode {
    /// Unknown configuration name.
    UnknownKey = -2,
    /// Invalid configuration value or
    /// property or value not supported in
    /// this build.
    InvalidValue = -1,
    /// Ok variant
    Ok = 0,
}

impl RDKafkaConfErrorCode {
    /// Returns an error if any
    pub fn error(&self) -> Option<&Self> {
        (!matches!(self, RDKafkaConfErrorCode::Ok)).then_some(self)
    }
}

impl From<rd_kafka_conf_res_t> for RDKafkaConfErrorCode {
    fn from(err: rd_kafka_conf_res_t) -> RDKafkaConfErrorCode {
        // UNWRAP: seemless conversion
        Self::try_from(err as i32).unwrap()
    }
}

impl fmt::Display for RDKafkaConfErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                RDKafkaConfErrorCode::UnknownKey => "Unknown configuration key name",
                RDKafkaConfErrorCode::InvalidValue =>
                    "Invalid configuration value or not supported",
                RDKafkaConfErrorCode::Ok => "Ok",
            }
        )
    }
}

impl Error for RDKafkaConfErrorCode {}

/// Events API event tags
#[derive(Debug, Clone, Copy, PartialEq, Eq, ::num_enum::TryFromPrimitive)]
#[repr(i32)]
#[non_exhaustive]
pub enum RDKafkaEventType {
    None = bindings::RD_KAFKA_EVENT_NONE,
    Dr = bindings::RD_KAFKA_EVENT_DR,
    Fetch = bindings::RD_KAFKA_EVENT_FETCH,
    Log = bindings::RD_KAFKA_EVENT_LOG,
    Error = bindings::RD_KAFKA_EVENT_ERROR,
    Rebalance = bindings::RD_KAFKA_EVENT_REBALANCE,
    //
    OffsetCommit = bindings::RD_KAFKA_EVENT_OFFSET_COMMIT,
    Stats = bindings::RD_KAFKA_EVENT_STATS,
    //
    CreateTopicsResult = bindings::RD_KAFKA_EVENT_CREATETOPICS_RESULT,
    DeleteTopicsResult = bindings::RD_KAFKA_EVENT_DELETETOPICS_RESULT,
    CreatePartitionsResult = bindings::RD_KAFKA_EVENT_CREATEPARTITIONS_RESULT,
    AlterConfigsResult = bindings::RD_KAFKA_EVENT_ALTERCONFIGS_RESULT,
    DescribeConfigsResult = bindings::RD_KAFKA_EVENT_DESCRIBECONFIGS_RESULT,
    DeleteRecordsResult = bindings::RD_KAFKA_EVENT_DELETERECORDS_RESULT,
    DeleteGroupsResult = bindings::RD_KAFKA_EVENT_DELETEGROUPS_RESULT,
    DeleteConsumerGroupOffsetsResult = bindings::RD_KAFKA_EVENT_DELETECONSUMERGROUPOFFSETS_RESULT,
    //
    OauthbearerTokenRefresh = bindings::RD_KAFKA_EVENT_OAUTHBEARER_TOKEN_REFRESH,
    Background = 0x200,
    //
    CreateAclsResult = bindings::RD_KAFKA_EVENT_CREATEACLS_RESULT,
    DescribeAclsResult = bindings::RD_KAFKA_EVENT_DESCRIBEACLS_RESULT,
    DeleteAclsResult = bindings::RD_KAFKA_EVENT_DELETEACLS_RESULT,
    ListConsumerGroupsResult = bindings::RD_KAFKA_EVENT_LISTCONSUMERGROUPS_RESULT,
    DescribeConsumerGroupsResult = bindings::RD_KAFKA_EVENT_DESCRIBECONSUMERGROUPS_RESULT,
    ListConsumerGroupOffsetsResult = bindings::RD_KAFKA_EVENT_LISTCONSUMERGROUPOFFSETS_RESULT,
    AlterConsumerGroupOffsetsResult = bindings::RD_KAFKA_EVENT_ALTERCONSUMERGROUPOFFSETS_RESULT,
    IncrementalAlterConfigsResult = bindings::RD_KAFKA_EVENT_INCREMENTALALTERCONFIGS_RESULT,
    //
    DescribeUserScramCredentialsResult =
        bindings::RD_KAFKA_EVENT_DESCRIBEUSERSCRAMCREDENTIALS_RESULT,
    AlterUserScramCredentialsResult = bindings::RD_KAFKA_EVENT_ALTERUSERSCRAMCREDENTIALS_RESULT,
    //
    DescribeTopicsResult = bindings::RD_KAFKA_EVENT_DESCRIBETOPICS_RESULT,
    DescribeClusterResult = bindings::RD_KAFKA_EVENT_DESCRIBECLUSTER_RESULT,
    ListOffsetsResult = bindings::RD_KAFKA_EVENT_LISTOFFSETS_RESULT,
    ElectLeadersResult = 0x800000,
}
