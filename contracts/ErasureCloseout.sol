// SPDX-License-Identifier: UNLICENSED
pragma solidity 0.8.35;

/// Experimental case registry. The assessor has already checked the evidence off-chain.
contract ErasureCloseout {
    enum Status { Unknown, Ready, Closed, Disputed }
    enum Decision { Recorded, Blocked, TooEarly }

    struct Case {
        bytes32 evidenceDigest;
        uint64 eligibleBlock;
        Status status;
        bool challenged;
        bool closeRecorded;
    }

    address immutable assessor;
    address immutable challenger;
    bool immutable guarded;
    uint64 immutable graceBlocks;
    mapping(bytes32 => Case) private cases;

    event Ready(bytes32 indexed caseId, bytes32 evidenceDigest, uint64 eligibleBlock);
    event Challenged(bytes32 indexed caseId, bytes32 challengeDigest);
    event CloseEvaluated(bytes32 indexed caseId, Decision decision);

    constructor(address challengerAddress, bool enforceGuard, uint64 delayBlocks) {
        require(challengerAddress != address(0) && challengerAddress != msg.sender, "challenger");
        assessor = msg.sender;
        challenger = challengerAddress;
        guarded = enforceGuard;
        graceBlocks = delayBlocks;
    }

    function registerReady(bytes32 caseId, bytes32 evidenceDigest) external {
        require(msg.sender == assessor, "assessor");
        require(cases[caseId].status == Status.Unknown && evidenceDigest != bytes32(0), "case");
        uint64 eligibleBlock = uint64(block.number) + graceBlocks;
        cases[caseId] = Case(evidenceDigest, eligibleBlock, Status.Ready, false, false);
        emit Ready(caseId, evidenceDigest, eligibleBlock);
    }

    function fileChallenge(bytes32 caseId, bytes32 challengeDigest) external {
        require(msg.sender == challenger, "challenger");
        Case storage entry = cases[caseId];
        require(entry.status != Status.Unknown && !entry.challenged, "case");
        require(challengeDigest != bytes32(0), "digest");
        entry.challenged = true;
        entry.status = Status.Disputed;
        emit Challenged(caseId, challengeDigest);
    }

    function recordClose(bytes32 caseId, bytes32 evidenceDigest) external {
        require(msg.sender == assessor, "assessor");
        Case storage entry = cases[caseId];
        require(entry.status != Status.Unknown && !entry.closeRecorded, "case");
        require(entry.evidenceDigest == evidenceDigest, "digest");
        if (block.number < entry.eligibleBlock) {
            emit CloseEvaluated(caseId, Decision.TooEarly);
        } else if (guarded && entry.challenged) {
            emit CloseEvaluated(caseId, Decision.Blocked);
        } else {
            // In preflight mode the assessor relies on an earlier off-chain read.
            entry.closeRecorded = true;
            entry.status = Status.Closed;
            emit CloseEvaluated(caseId, Decision.Recorded);
        }
    }

    function statusOf(bytes32 caseId) external view returns (Status) {
        return cases[caseId].status;
    }
}
