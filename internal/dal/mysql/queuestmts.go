package mysql

import (
	"github.com/yakugate/yakugate/internal/dal/stmt"
)

// QueueStmts is the queue store's statements on the message path, by name
// (step 3 of the DAL plan, 30 Oct 2026): the client receipt and MO claims,
// leases and acks, the receipt enqueue and the outgoing queue's take. A
// statement built at the call (an IN list, the owner's condition, one
// account's attempt window) is registered in its one-account form; the text a
// call sends is that form with more values. Each call is counted on its
// statement (Observe).
var QueueStmts = stmt.New("mysql")

// The client receipt claim reads one account's rows by idx_cdlr_claim
// (queue/0047) in id order and stops when its page is full.
const cdlrClaimHead = `SELECT /*+ INDEX(client_dlr idx_cdlr_claim) */ id,account_id,client_msg_id,status,COALESCE(stat,''),COALESCE(receipt,''),
		       attempts,created_at,COALESCE(bind_slot,0),COALESCE(src,''),COALESCE(dst,''),COALESCE(queue_owner,''),
		       COALESCE(done_at_ms,0),COALESCE(msg_id,'')
		FROM client_dlr`

// The MO claim reads one account's rows by idx_mo_claim (queue/0047) in id
// order and stops when its page is full. It reads the MO's price too
// (queue/0048): a record written by its delivery has it.
const moClaimHead = `SELECT /*+ INDEX(mo_queue idx_mo_claim) */ msg_id,vendor_id,account_id,COALESCE(route_id,0),sender,destination,body,
		encoding,segments,COALESCE(mcc,''),COALESCE(mnc,''),attempts,received_at,COALESCE(queue_owner,''),
		COALESCE(company_id,0),COALESCE(priced,'') FROM mo_queue`

// The short codes (and owners) of a vendor's table that hold rows: read by
// idx_pick's first two columns only, one index dive per pair ("Using index
// for group-by"). It read every queued row of the owner (a DISTINCT over the
// index range: 2 million index rows in a 3-minute rig run).
const mtSubQueuesShared = "SELECT DISTINCT queue_owner, shortcode FROM `queue_<cid>` WHERE queue_owner IS NULL"
const mtSubQueuesOwn = "SELECT DISTINCT queue_owner, shortcode FROM `queue_<cid>` WHERE queue_owner = ? OR queue_owner IS NULL"

var (
	stCDLREnqueue = QueueStmts.Add("queue.client_dlr.enqueue",
		`INSERT INTO client_dlr (account_id,client_msg_id,status,stat,receipt,bind_slot,src,dst,queue_owner,done_at_ms,msg_id,next_attempt_at,created_at)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,DATE_ADD(NOW(),INTERVAL ? SECOND),DATE_ADD(NOW(),INTERVAL ? SECOND))`, true)
	stCDLRClaim = QueueStmts.Add("queue.client_dlr.claim", cdlrClaimHead+`
		WHERE (account_id IN (?)) AND given_up_at IS NULL
		  AND (next_attempt_at IS NULL OR next_attempt_at<=NOW()) AND (queue_owner = ? OR queue_owner IS NULL)
		ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED`, true)
	stCDLRLease = QueueStmts.Add("queue.client_dlr.lease",
		`UPDATE client_dlr SET attempts=attempts+1, next_attempt_at=DATE_ADD(NOW(),INTERVAL 60 SECOND) WHERE id IN (?)`, true)
	stCDLRAck = QueueStmts.Add("queue.client_dlr.ack", `DELETE FROM client_dlr WHERE id IN (?)`, true)

	stMOClaim = QueueStmts.Add("queue.mo.claim", moClaimHead+`
		WHERE status='queued' AND (account_id IN (?)) AND (next_attempt_at IS NULL OR next_attempt_at<=NOW())
		  AND (queue_owner = ? OR queue_owner IS NULL)
		ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED`, true)
	stMOLease = QueueStmts.Add("queue.mo.lease",
		`UPDATE mo_queue SET attempts=attempts+1, next_attempt_at=DATE_ADD(NOW(),INTERVAL 60 SECOND) WHERE msg_id IN (?)`, true)
	stMOAck = QueueStmts.Add("queue.mo.ack", `DELETE FROM mo_queue WHERE msg_id IN (?)`, true)

	stMTSubQueues = QueueStmts.Add("queue.mt.sub_queues", mtSubQueuesOwn, true)
	stMTReady     = QueueStmts.Add("queue.mt.ready", "SELECT priority, COUNT(*) FROM "+
		"(SELECT priority FROM `queue_<cid>` WHERE queue_owner <=> ? AND shortcode = ? AND "+readyCond+" ORDER BY priority LIMIT ?) r GROUP BY priority", true)
	stMTTake = QueueStmts.Add("queue.mt.take", "SELECT "+queueSelectCols+" FROM `queue_<cid>`"+
		" WHERE queue_owner <=> ? AND shortcode = ? AND "+readyCond+" ORDER BY priority, pick_at, id LIMIT ? FOR UPDATE SKIP LOCKED", true)
	stMTDelete = QueueStmts.Add("queue.mt.delete", "DELETE FROM `queue_<cid>` WHERE id IN (?)", true)
)
