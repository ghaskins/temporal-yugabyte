// The MIT License
//
// Copyright (c) 2025 Manetu Inc.  All rights reserved.
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package driver

import (
	"context"
	"fmt"
	"github.com/manetu/temporal-yugabyte/utils/gocql"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	p "go.temporal.io/server/common/persistence"

	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives/timestamp"
)

const (
	templateGetLeaseQuery = `SELECT range_id FROM shards ` +
		`WHERE shard_id = ? `

	templateUpdateLeaseQuery = `UPDATE shards ` +
		`SET range_id = ? ` +
		`WHERE shard_id = ? ` +
		`IF range_id = ? ELSE ERROR `

	templateUpdateCurrentWorkflowExecutionQueryPrefix = `UPDATE current_executions USING TTL 0 ` +
		`SET current_run_id = ?, execution_state = ?, execution_state_encoding = ?, workflow_last_write_version = ?, workflow_state = ? ` +
		`WHERE shard_id = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? `

	templateUpdateCurrentWorkflowExecutionQuery = templateUpdateCurrentWorkflowExecutionQueryPrefix +
		`IF current_run_id = ? ELSE ERROR `

	templateUpdateCurrentWorkflowExecutionForNewQuery = templateUpdateCurrentWorkflowExecutionQueryPrefix +
		`IF workflow_last_write_version = ? ` +
		`and workflow_state = ? ` +
		`and current_run_id = ? ELSE ERROR `

	templateCreateCurrentWorkflowExecutionQuery = `INSERT INTO current_executions (` +
		`shard_id, namespace_id, workflow_id, ` +
		`current_run_id, execution_state, execution_state_encoding, ` +
		`workflow_last_write_version, workflow_state) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS ELSE ERROR USING TTL 0 `

	templateCreateWorkflowExecutionQuery = `INSERT INTO executions (` +
		`shard_id, namespace_id, workflow_id, run_id, ` +
		`execution, execution_encoding, execution_state, execution_state_encoding, next_event_id, db_record_version, ` +
		`checksum, checksum_encoding) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS ELSE ERROR `

	templateGetWorkflowExecutionQuery = `SELECT execution, execution_encoding, execution_state, execution_state_encoding, next_event_id, activity_map, activity_map_encoding, timer_map, timer_map_encoding, ` +
		`child_executions_map, child_executions_map_encoding, request_cancel_map, request_cancel_map_encoding, signal_map, signal_map_encoding, signal_requested, buffered_events_list, ` +
		`checksum, checksum_encoding, db_record_version ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? `

	templateGetWorkflowExecutionConflictsQuery = `SELECT run_id, db_record_version, next_event_id ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? `

	templateGetCurrentExecutionQuery = `SELECT current_run_id, execution_state, execution_state_encoding, workflow_last_write_version ` +
		`FROM current_executions ` +
		`WHERE shard_id = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? `

	templateGetCurrentExecutionConflictQuery = `SELECT current_run_id, execution_state, execution_state_encoding, workflow_state, workflow_last_write_version ` +
		`FROM current_executions ` +
		`WHERE shard_id = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? `

	templateListWorkflowExecutionQuery = `SELECT run_id, execution, execution_encoding, execution_state, execution_state_encoding, next_event_id ` +
		`FROM executions ` +
		`WHERE shard_id = ? `

	// TODO deprecate templateUpdateWorkflowExecutionQueryDeprecated in favor of templateUpdateWorkflowExecutionQuery
	// Deprecated.
	templateUpdateWorkflowExecutionQueryDeprecated = `UPDATE executions ` +
		`SET execution = ? ` +
		`, execution_encoding = ? ` +
		`, execution_state = ? ` +
		`, execution_state_encoding = ? ` +
		`, next_event_id = ? ` +
		`, db_record_version = ? ` +
		`, checksum = ? ` +
		`, checksum_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`IF next_event_id = ? ELSE ERROR `
	templateUpdateWorkflowExecutionQuery = `UPDATE executions ` +
		`SET execution = ? ` +
		`, execution_encoding = ? ` +
		`, execution_state = ? ` +
		`, execution_state_encoding = ? ` +
		`, next_event_id = ? ` +
		`, db_record_version = ? ` +
		`, checksum = ? ` +
		`, checksum_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`IF db_record_version = ? ELSE ERROR `

	templateUpdateActivityInfoQuery = `UPDATE executions ` +
		`SET activity_map[ ? ] = ?, activity_map_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? `

	templateResetActivityInfoQuery = `UPDATE executions ` +
		`SET activity_map = ?, activity_map_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? `

	templateUpdateTimerInfoQuery = `UPDATE executions ` +
		`SET timer_map[ ? ] = ?, timer_map_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? `

	templateResetTimerInfoQuery = `UPDATE executions ` +
		`SET timer_map = ?, timer_map_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? `

	templateUpdateChildExecutionInfoQuery = `UPDATE executions ` +
		`SET child_executions_map[ ? ] = ?, child_executions_map_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? `

	templateResetChildExecutionInfoQuery = `UPDATE executions ` +
		`SET child_executions_map = ?, child_executions_map_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? `

	templateUpdateRequestCancelInfoQuery = `UPDATE executions ` +
		`SET request_cancel_map[ ? ] = ?, request_cancel_map_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? `

	templateResetRequestCancelInfoQuery = `UPDATE executions ` +
		`SET request_cancel_map = ?, request_cancel_map_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? `

	templateUpdateSignalInfoQuery = `UPDATE executions ` +
		`SET signal_map[ ? ] = ?, signal_map_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? `

	templateResetSignalInfoQuery = `UPDATE executions ` +
		`SET signal_map = ?, signal_map_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? `

	templateUpdateSignalRequestedQuery = `UPDATE executions ` +
		`SET signal_requested = signal_requested + ? ` +
		`WHERE shard_id = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? `

	templateResetSignalRequestedQuery = `UPDATE executions ` +
		`SET signal_requested = ?` +
		`WHERE shard_id = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? `

	templateAppendBufferedEventsQuery = `UPDATE executions ` +
		`SET buffered_events_list = buffered_events_list + ? ` +
		`WHERE shard_id = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? `

	templateDeleteBufferedEventsQuery = `UPDATE executions ` +
		`SET buffered_events_list = [] ` +
		`WHERE shard_id = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? `

	templateDeleteActivityInfoQuery = `DELETE activity_map[ ? ] ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? `

	templateDeleteTimerInfoQuery = `DELETE timer_map[ ? ] ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? `

	templateDeleteChildExecutionInfoQuery = `DELETE child_executions_map[ ? ] ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? `

	templateDeleteRequestCancelInfoQuery = `DELETE request_cancel_map[ ? ] ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? `

	templateDeleteSignalInfoQuery = `DELETE signal_map[ ? ] ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? `

	templateDeleteWorkflowExecutionMutableStateQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? `

	templateDeleteCurrentWorkflowExecutionQuery = `DELETE FROM current_executions ` +
		`WHERE shard_id = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`IF current_run_id = ? `

	templateDeleteWorkflowExecutionSignalRequestedQuery = `UPDATE executions ` +
		`SET signal_requested = signal_requested - ? ` +
		`WHERE shard_id = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? `
)

type (
	MutableStateStore struct {
		Session gocql.Session
	}
)

func NewMutableStateStore(session gocql.Session) *MutableStateStore {
	return &MutableStateStore{
		Session: session,
	}
}

func (d *MutableStateStore) validateCurrentWorkflowNotExist(
	ctx context.Context,
	validator *PostQueryValidation,
	shardID int32,
	namespaceID string,
	workflowID string) {

	validator.Add(func() error {
		conflictRecord := newConflictRecord()
		err := d.Session.Query(templateGetCurrentExecutionConflictQuery,
			shardID,
			namespaceID,
			workflowID).WithContext(ctx).MapScan(conflictRecord)
		if gocql.IsNotFoundError(err) {
			return nil
		} else if err == nil {
			return exportCurrentWorkflowConflictError(conflictRecord)
		}
		return nil
	})
}

func (d *MutableStateStore) validateCurrentWorkflowRunId(
	ctx context.Context,
	validator *PostQueryValidation,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string) {

	validator.Add(func() error {
		conflictRecord := newConflictRecord()
		err := d.Session.Query(templateGetCurrentExecutionConflictQuery,
			shardID,
			namespaceID,
			workflowID).WithContext(ctx).MapScan(conflictRecord)
		if err == nil {
			if runID != gocql.UUIDToString(conflictRecord["current_run_id"]) {
				return exportCurrentWorkflowConflictError(conflictRecord)
			} else {
				return nil
			}
		}
		return err
	})
}

func (d *MutableStateStore) validateCurrentWorkflowMvcc(
	ctx context.Context,
	validator *PostQueryValidation,
	shardID int32,
	namespaceID string,
	workflowID string,
	lastWriteVersion int64,
	workflowState enumsspb.WorkflowExecutionState,
	runID string) {

	validator.Add(func() error {
		conflictRecord := newConflictRecord()
		err := d.Session.Query(templateGetCurrentExecutionConflictQuery,
			shardID,
			namespaceID,
			workflowID).WithContext(ctx).MapScan(conflictRecord)
		if err == nil {
			actualRunID := gocql.UUIDToString(conflictRecord["current_run_id"])
			actualLastWriteVersion := conflictRecord["workflow_last_write_version"].(int64)
			requestWorkflowState := int(workflowState)
			actualWorkflowState := conflictRecord["workflow_state"]
			if runID != actualRunID ||
				lastWriteVersion != actualLastWriteVersion ||
				requestWorkflowState != actualWorkflowState {
				return exportCurrentWorkflowConflictError(conflictRecord)
			} else {
				return nil
			}
		}
		return err
	})
}

func (d *MutableStateStore) decodeConflict(
	ctx context.Context,
	validator *PostQueryValidation,
	shardID int32,
	namespaceID string,
	workflowID string,
	rangeID int64,
	requestExecutionCASConditions []executionCASCondition,
) error {

	// Check Ownership for conflicts
	validator.Add(func() error {
		conflictRecord := newConflictRecord()
		err := d.Session.Query(templateGetLeaseQuery,
			shardID).WithContext(ctx).MapScan(conflictRecord)
		if err == nil {
			return extractShardOwnershipLostError(conflictRecord, shardID, rangeID)
		}

		return err
	})

	// Check Execution for conflicts
	for _, condition := range requestExecutionCASConditions {
		validator.Add(func() error {
			conflictRecord := newConflictRecord()
			err := d.Session.Query(templateGetWorkflowExecutionConflictsQuery,
				shardID,
				namespaceID,
				workflowID,
				condition.runID).WithContext(ctx).MapScan(conflictRecord)
			if err == nil {
				return extractWorkflowConflictError(
					conflictRecord,
					condition.dbVersion,
					condition.nextEventID,
				)
			} else if gocql.IsNotFoundError(err) {
				return &p.ConditionFailedError{
					Msg: fmt.Sprintf("Workflow does not exist: %v", workflowID),
				}
			}

			return err

		})
	}

	err := validator.Validate()
	if err != nil {
		return err
	}

	return &p.ConditionFailedError{
		Msg: fmt.Sprintf("WorkflowExecution conflict: Shard:%v, Namespace:%v, WorkflowID:%v",
			shardID, namespaceID, workflowID),
	}
}

func (d *MutableStateStore) CreateWorkflowExecution(
	ctx context.Context,
	request *p.InternalCreateWorkflowExecutionRequest,
) (*p.InternalCreateWorkflowExecutionResponse, error) {
	txn := d.Session.NewTxn().WithContext(ctx)
	validator := NewPostQueryValidation()

	shardID := request.ShardID
	newWorkflow := request.NewWorkflowSnapshot
	lastWriteVersion := newWorkflow.LastWriteVersion
	namespaceID := newWorkflow.NamespaceID
	workflowID := newWorkflow.WorkflowID
	runID := newWorkflow.RunID

	switch request.Mode {
	case p.CreateWorkflowModeBypassCurrent:
		// noop

	case p.CreateWorkflowModeUpdateCurrent:
		txn.Query(templateUpdateCurrentWorkflowExecutionForNewQuery,
			runID,
			newWorkflow.ExecutionStateBlob.Data,
			newWorkflow.ExecutionStateBlob.EncodingType.String(),
			lastWriteVersion,
			newWorkflow.ExecutionState.State,
			shardID,
			namespaceID,
			workflowID,
			request.PreviousLastWriteVersion,
			enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			request.PreviousRunID,
		)

		d.validateCurrentWorkflowMvcc(ctx,
			validator,
			shardID,
			namespaceID,
			workflowID,
			request.PreviousLastWriteVersion,
			enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			request.PreviousRunID)

	case p.CreateWorkflowModeBrandNew:
		txn.Query(templateCreateCurrentWorkflowExecutionQuery,
			shardID,
			namespaceID,
			workflowID,
			runID,
			newWorkflow.ExecutionStateBlob.Data,
			newWorkflow.ExecutionStateBlob.EncodingType.String(),
			lastWriteVersion,
			newWorkflow.ExecutionState.State,
		)

		d.validateCurrentWorkflowNotExist(ctx, validator, shardID, namespaceID, workflowID)

	default:
		return nil, serviceerror.NewInternal(fmt.Sprintf("CreateWorkflowExecution: unknown mode: %v", request.Mode))
	}

	if err := applyWorkflowSnapshotTxnAsNew(txn,
		request.ShardID,
		&newWorkflow,
	); err != nil {
		return nil, err
	}

	txn.Query(templateUpdateLeaseQuery,
		request.RangeID,
		request.ShardID,
		request.RangeID,
	)

	err := txn.Exec()
	if err != nil {
		if gocql.ConflictError(err) {
			return nil, d.decodeConflict(
				ctx,
				validator,
				shardID,
				namespaceID,
				workflowID,
				request.RangeID,
				[]executionCASCondition{{
					runID: newWorkflow.ExecutionState.RunId,
					// dbVersion is for CAS, so the db record version will be set to `updateWorkflow.DBRecordVersion`
					// while CAS on `updateWorkflow.DBRecordVersion - 1`
					dbVersion:   newWorkflow.DBRecordVersion - 1,
					nextEventID: newWorkflow.Condition,
				}},
			)
		}
		return nil, err
	}

	return &p.InternalCreateWorkflowExecutionResponse{}, nil
}

func (d *MutableStateStore) GetWorkflowExecution(
	ctx context.Context,
	request *p.GetWorkflowExecutionRequest,
) (*p.InternalGetWorkflowExecutionResponse, error) {
	query := d.Session.Query(templateGetWorkflowExecutionQuery,
		request.ShardID,
		request.NamespaceID,
		request.WorkflowID,
		request.RunID,
	).WithContext(ctx)

	result := make(map[string]interface{})
	if err := query.MapScan(result); err != nil {
		return nil, gocql.ConvertError("GetWorkflowExecution", err)
	}

	state, err := mutableStateFromRow(result)
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetWorkflowExecution operation failed. Error: %v", err))
	}

	activityInfos := make(map[int64]*commonpb.DataBlob)
	aMap := result["activity_map"].(map[int64][]byte)
	aMapEncoding := result["activity_map_encoding"].(string)
	for key, value := range aMap {
		activityInfos[key] = p.NewDataBlob(value, aMapEncoding)
	}
	state.ActivityInfos = activityInfos

	timerInfos := make(map[string]*commonpb.DataBlob)
	tMapEncoding := result["timer_map_encoding"].(string)
	tMap := result["timer_map"].(map[string][]byte)
	for key, value := range tMap {
		timerInfos[key] = p.NewDataBlob(value, tMapEncoding)
	}
	state.TimerInfos = timerInfos

	childExecutionInfos := make(map[int64]*commonpb.DataBlob)
	cMap := result["child_executions_map"].(map[int64][]byte)
	cMapEncoding := result["child_executions_map_encoding"].(string)
	for key, value := range cMap {
		childExecutionInfos[key] = p.NewDataBlob(value, cMapEncoding)
	}
	state.ChildExecutionInfos = childExecutionInfos

	requestCancelInfos := make(map[int64]*commonpb.DataBlob)
	rMapEncoding := result["request_cancel_map_encoding"].(string)
	rMap := result["request_cancel_map"].(map[int64][]byte)
	for key, value := range rMap {
		requestCancelInfos[key] = p.NewDataBlob(value, rMapEncoding)
	}
	state.RequestCancelInfos = requestCancelInfos

	signalInfos := make(map[int64]*commonpb.DataBlob)
	sMapEncoding := result["signal_map_encoding"].(string)
	sMap := result["signal_map"].(map[int64][]byte)
	for key, value := range sMap {
		signalInfos[key] = p.NewDataBlob(value, sMapEncoding)
	}
	state.SignalInfos = signalInfos
	state.SignalRequestedIDs = gocql.UUIDsToStringSlice(result["signal_requested"])

	eList := result["buffered_events_list"].([]map[string]interface{})
	bufferedEventsBlobs := make([]*commonpb.DataBlob, 0, len(eList))
	for _, v := range eList {
		blob := createHistoryEventBatchBlob(v)
		bufferedEventsBlobs = append(bufferedEventsBlobs, blob)
	}
	state.BufferedEvents = bufferedEventsBlobs

	state.Checksum = p.NewDataBlob(result["checksum"].([]byte), result["checksum_encoding"].(string))

	dbVersion := int64(0)
	if dbRecordVersion, ok := result["db_record_version"]; ok {
		dbVersion = dbRecordVersion.(int64)
	} else {
		dbVersion = 0
	}

	return &p.InternalGetWorkflowExecutionResponse{
		State:           state,
		DBRecordVersion: dbVersion,
	}, nil
}

func (d *MutableStateStore) UpdateWorkflowExecution(
	ctx context.Context,
	request *p.InternalUpdateWorkflowExecutionRequest,
) error {
	txn := d.Session.NewTxn().WithContext(ctx)
	validator := NewPostQueryValidation()

	updateWorkflow := request.UpdateWorkflowMutation
	newWorkflow := request.NewWorkflowSnapshot

	namespaceID := updateWorkflow.NamespaceID
	workflowID := updateWorkflow.WorkflowID
	runID := updateWorkflow.RunID
	shardID := request.ShardID

	switch request.Mode {
	case p.UpdateWorkflowModeBypassCurrent:
		if err := d.assertNotCurrentExecution(
			ctx,
			request.ShardID,
			namespaceID,
			workflowID,
			runID,
			timestamp.TimeValuePtr(updateWorkflow.ExecutionState.StartTime),
		); err != nil {
			return err
		}

	case p.UpdateWorkflowModeUpdateCurrent:
		if newWorkflow != nil {
			newLastWriteVersion := newWorkflow.LastWriteVersion
			newNamespaceID := newWorkflow.NamespaceID
			newWorkflowID := newWorkflow.WorkflowID
			newRunID := newWorkflow.RunID

			if namespaceID != newNamespaceID {
				return serviceerror.NewInternal("UpdateWorkflowExecution: cannot continue as new to another namespace")
			}

			txn.Query(templateUpdateCurrentWorkflowExecutionQuery,
				newRunID,
				newWorkflow.ExecutionStateBlob.Data,
				newWorkflow.ExecutionStateBlob.EncodingType.String(),
				newLastWriteVersion,
				newWorkflow.ExecutionState.State,
				shardID,
				newNamespaceID,
				newWorkflowID,
				runID,
			)

			d.validateCurrentWorkflowRunId(ctx,
				validator,
				shardID,
				namespaceID,
				workflowID,
				runID)

		} else {
			lastWriteVersion := updateWorkflow.LastWriteVersion

			// TODO: double encoding execution state? already in updateWorkflow.ExecutionStateBlob
			executionStateDatablob, err := serialization.WorkflowExecutionStateToBlob(updateWorkflow.ExecutionState)
			if err != nil {
				return err
			}

			txn.Query(templateUpdateCurrentWorkflowExecutionQuery,
				runID,
				executionStateDatablob.Data,
				executionStateDatablob.EncodingType.String(),
				lastWriteVersion,
				updateWorkflow.ExecutionState.State,
				request.ShardID,
				namespaceID,
				workflowID,
				runID,
			)

			d.validateCurrentWorkflowRunId(ctx,
				validator,
				shardID,
				namespaceID,
				workflowID,
				runID)
		}

	default:
		return serviceerror.NewInternal(fmt.Sprintf("UpdateWorkflowExecution: unknown mode: %v", request.Mode))
	}

	if err := applyWorkflowMutationTxn(txn, shardID, &updateWorkflow); err != nil {
		return err
	}
	if newWorkflow != nil {
		if err := applyWorkflowSnapshotTxnAsNew(txn,
			request.ShardID,
			newWorkflow,
		); err != nil {
			return err
		}
	}

	// Verifies that the RangeID has not changed
	txn.Query(templateUpdateLeaseQuery,
		request.RangeID,
		request.ShardID,
		request.RangeID,
	)

	err := txn.Exec()
	if err != nil {
		if gocql.ConflictError(err) {
			return d.decodeConflict(
				ctx,
				validator,
				shardID,
				namespaceID,
				workflowID,
				request.RangeID,
				[]executionCASCondition{{
					runID: updateWorkflow.ExecutionState.RunId,
					// dbVersion is for CAS, so the db record version will be set to `updateWorkflow.DBRecordVersion`
					// while CAS on `updateWorkflow.DBRecordVersion - 1`
					dbVersion:   updateWorkflow.DBRecordVersion - 1,
					nextEventID: updateWorkflow.Condition,
				}})
		}
		return err
	}

	return nil
}

func (d *MutableStateStore) ConflictResolveWorkflowExecution(
	ctx context.Context,
	request *p.InternalConflictResolveWorkflowExecutionRequest,
) error {
	txn := d.Session.NewTxn().WithContext(ctx)
	validator := NewPostQueryValidation()

	currentWorkflow := request.CurrentWorkflowMutation
	resetWorkflow := request.ResetWorkflowSnapshot
	newWorkflow := request.NewWorkflowSnapshot

	shardID := request.ShardID

	namespaceID := resetWorkflow.NamespaceID
	workflowID := resetWorkflow.WorkflowID

	var currentRunID string

	var startTime *time.Time
	if currentWorkflow != nil && currentWorkflow.ExecutionState != nil {
		startTime = timestamp.TimeValuePtr(currentWorkflow.ExecutionState.StartTime)
	}

	switch request.Mode {
	case p.ConflictResolveWorkflowModeBypassCurrent:
		if err := d.assertNotCurrentExecution(
			ctx,
			shardID,
			namespaceID,
			workflowID,
			resetWorkflow.ExecutionState.RunId,
			startTime,
		); err != nil {
			return err
		}

	case p.ConflictResolveWorkflowModeUpdateCurrent:
		executionState := resetWorkflow.ExecutionState
		executionStateBlob := resetWorkflow.ExecutionStateBlob
		lastWriteVersion := resetWorkflow.LastWriteVersion
		if newWorkflow != nil {
			lastWriteVersion = newWorkflow.LastWriteVersion
			executionState = newWorkflow.ExecutionState
			executionStateBlob = newWorkflow.ExecutionStateBlob
		}

		if currentWorkflow != nil {
			currentRunID = currentWorkflow.ExecutionState.RunId
		} else {
			// reset workflow is current
			currentRunID = resetWorkflow.ExecutionState.RunId
		}

		txn.Query(templateUpdateCurrentWorkflowExecutionQuery,
			executionState.RunId,
			executionStateBlob.Data,
			executionStateBlob.EncodingType.String(),
			lastWriteVersion,
			executionState.State,
			shardID,
			namespaceID,
			workflowID,
			currentRunID,
		)

		d.validateCurrentWorkflowRunId(ctx,
			validator,
			shardID,
			namespaceID,
			workflowID,
			currentRunID)

	default:
		return serviceerror.NewInternal(fmt.Sprintf("ConflictResolveWorkflowExecution: unknown mode: %v", request.Mode))
	}

	if err := applyWorkflowSnapshotTxnAsReset(txn, shardID, &resetWorkflow); err != nil {
		return err
	}

	if currentWorkflow != nil {
		if err := applyWorkflowMutationTxn(txn, shardID, currentWorkflow); err != nil {
			return err
		}
	}
	if newWorkflow != nil {
		if err := applyWorkflowSnapshotTxnAsNew(txn, shardID, newWorkflow); err != nil {
			return err
		}
	}

	// Verifies that the RangeID has not changed
	txn.Query(templateUpdateLeaseQuery,
		request.RangeID,
		request.ShardID,
		request.RangeID,
	)

	err := txn.Exec()
	if err != nil {
		if gocql.ConflictError(err) {
			executionCASConditions := []executionCASCondition{{
				runID: resetWorkflow.RunID,
				// dbVersion is for CAS, so the db record version will be set to `resetWorkflow.DBRecordVersion`
				// while CAS on `resetWorkflow.DBRecordVersion - 1`
				dbVersion:   resetWorkflow.DBRecordVersion - 1,
				nextEventID: resetWorkflow.Condition,
			}}
			if currentWorkflow != nil {
				executionCASConditions = append(executionCASConditions, executionCASCondition{
					runID: currentWorkflow.RunID,
					// dbVersion is for CAS, so the db record version will be set to `currentWorkflow.DBRecordVersion`
					// while CAS on `currentWorkflow.DBRecordVersion - 1`
					dbVersion:   currentWorkflow.DBRecordVersion - 1,
					nextEventID: currentWorkflow.Condition,
				})
			}
			return d.decodeConflict(
				ctx,
				validator,
				shardID,
				namespaceID,
				workflowID,
				request.RangeID,
				executionCASConditions)
		}
		return err
	}

	return nil
}

func (d *MutableStateStore) assertNotCurrentExecution(
	ctx context.Context,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
	startTime *time.Time,
) error {

	if resp, err := d.GetCurrentExecution(ctx, &p.GetCurrentExecutionRequest{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}); err != nil {
		if _, isNotFound := err.(*serviceerror.NotFound); isNotFound {
			// allow bypassing no current record
			return nil
		}
		return err
	} else if resp.RunID == runID {
		return &p.CurrentWorkflowConditionFailedError{
			Msg:              fmt.Sprintf("Assertion on current record failed. Current run ID is not expected: %v", resp.RunID),
			RequestIDs:       nil,
			RunID:            "",
			State:            enumsspb.WORKFLOW_EXECUTION_STATE_UNSPECIFIED,
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED,
			LastWriteVersion: 0,
			StartTime:        startTime,
		}
	}

	return nil
}

func (d *MutableStateStore) DeleteWorkflowExecution(
	ctx context.Context,
	request *p.DeleteWorkflowExecutionRequest,
) error {
	query := d.Session.Query(templateDeleteWorkflowExecutionMutableStateQuery,
		request.ShardID,
		request.NamespaceID,
		request.WorkflowID,
		request.RunID,
	).WithContext(ctx)

	err := query.Exec()
	return gocql.ConvertError("DeleteWorkflowExecution", err)
}

func (d *MutableStateStore) DeleteCurrentWorkflowExecution(
	ctx context.Context,
	request *p.DeleteCurrentWorkflowExecutionRequest,
) error {
	query := d.Session.Query(templateDeleteCurrentWorkflowExecutionQuery,
		request.ShardID,
		request.NamespaceID,
		request.WorkflowID,
		request.RunID,
	).WithContext(ctx)

	err := query.Exec()
	return gocql.ConvertError("DeleteWorkflowCurrentRow", err)
}

func (d *MutableStateStore) GetCurrentExecution(
	ctx context.Context,
	request *p.GetCurrentExecutionRequest,
) (*p.InternalGetCurrentExecutionResponse, error) {
	query := d.Session.Query(templateGetCurrentExecutionQuery,
		request.ShardID,
		request.NamespaceID,
		request.WorkflowID,
	).WithContext(ctx)

	result := make(map[string]interface{})
	if err := query.MapScan(result); err != nil {
		return nil, gocql.ConvertError("GetCurrentExecution", err)
	}

	currentRunID := gocql.UUIDToString(result["current_run_id"])
	executionStateBlob, err := executionStateBlobFromRow(result)
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetCurrentExecution operation failed. Error: %v", err))
	}

	// TODO: fix blob ExecutionState in storage should not be a blob.
	executionState, err := serialization.WorkflowExecutionStateFromBlob(executionStateBlob.Data, executionStateBlob.EncodingType.String())
	if err != nil {
		return nil, err
	}

	return &p.InternalGetCurrentExecutionResponse{
		RunID:          currentRunID,
		ExecutionState: executionState,
	}, nil
}

func (d *MutableStateStore) SetWorkflowExecution(
	ctx context.Context,
	request *p.InternalSetWorkflowExecutionRequest,
) error {
	txn := d.Session.NewTxn().WithContext(ctx)
	validator := NewPostQueryValidation()

	shardID := request.ShardID
	setSnapshot := request.SetWorkflowSnapshot

	if err := applyWorkflowSnapshotTxnAsReset(txn, shardID, &setSnapshot); err != nil {
		return err
	}

	// Verifies that the RangeID has not changed
	txn.Query(templateUpdateLeaseQuery,
		request.RangeID,
		request.ShardID,
		request.RangeID,
	)

	err := txn.Exec()
	if err != nil {
		if gocql.ConflictError(err) {
			executionCASConditions := []executionCASCondition{{
				runID: setSnapshot.RunID,
				// dbVersion is for CAS, so the db record version will be set to `setSnapshot.DBRecordVersion`
				// while CAS on `setSnapshot.DBRecordVersion - 1`
				dbVersion:   setSnapshot.DBRecordVersion - 1,
				nextEventID: setSnapshot.Condition,
			}}
			return d.decodeConflict(
				ctx,
				validator,
				request.ShardID,
				request.SetWorkflowSnapshot.NamespaceID,
				request.SetWorkflowSnapshot.WorkflowID,
				request.RangeID,
				executionCASConditions)
		}
		return err
	}

	return nil
}

func (d *MutableStateStore) ListConcreteExecutions(
	ctx context.Context,
	request *p.ListConcreteExecutionsRequest,
) (*p.InternalListConcreteExecutionsResponse, error) {
	query := d.Session.Query(templateListWorkflowExecutionQuery,
		request.ShardID,
	).WithContext(ctx)
	iter := query.PageSize(request.PageSize).PageState(request.PageToken).Iter()

	response := &p.InternalListConcreteExecutionsResponse{}
	result := make(map[string]interface{})
	for iter.MapScan(result) {
		if _, ok := result["execution"]; ok {
			state, err := mutableStateFromRow(result)
			if err != nil {
				return nil, err
			}
			response.States = append(response.States, state)
		}
		result = make(map[string]interface{})
	}
	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}
	return response, nil
}

func mutableStateFromRow(
	result map[string]interface{},
) (*p.InternalWorkflowMutableState, error) {
	eiBytes, ok := result["execution"].([]byte)
	if !ok {
		return nil, newPersistedTypeMismatchError("execution", "", eiBytes, result)
	}

	eiEncoding, ok := result["execution_encoding"].(string)
	if !ok {
		return nil, newPersistedTypeMismatchError("execution_encoding", "", eiEncoding, result)
	}

	nextEventID, ok := result["next_event_id"].(int64)
	if !ok {
		return nil, newPersistedTypeMismatchError("next_event_id", "", nextEventID, result)
	}

	protoState, err := executionStateBlobFromRow(result)
	if err != nil {
		return nil, err
	}

	mutableState := &p.InternalWorkflowMutableState{
		ExecutionInfo:  p.NewDataBlob(eiBytes, eiEncoding),
		ExecutionState: protoState,
		NextEventID:    nextEventID,
	}
	return mutableState, nil
}

func executionStateBlobFromRow(
	result map[string]interface{},
) (*commonpb.DataBlob, error) {
	state, ok := result["execution_state"].([]byte)
	if !ok {
		return nil, newPersistedTypeMismatchError("execution_state", "", state, result)
	}

	stateEncoding, ok := result["execution_state_encoding"].(string)
	if !ok {
		return nil, newPersistedTypeMismatchError("execution_state_encoding", "", stateEncoding, result)
	}

	return p.NewDataBlob(state, stateEncoding), nil
}
