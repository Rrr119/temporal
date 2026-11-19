package worker_versioning

import (
	"math"

	"github.com/dgryski/go-farm"
	"go.temporal.io/api/serviceerror"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
)

var (
	errWorkflowTypeRoutingNoClock        = serviceerror.NewInternal("workflow type routing update requires a clock")
	errWorkflowTypeRoutingEmptyType      = serviceerror.NewInvalidArgument("workflow type cannot be empty")
	errWorkflowTypeRoutingInvalidPercent = serviceerror.NewInvalidArgument("workflow type routing percentage must be in range [0, 100]")
	errWorkflowTypeRoutingInvalidVersion = serviceerror.NewInvalidArgument("workflow type routing target must have both deployment name and build ID, or neither")
)

// SetWorkflowTypeRoutingRule appends a workflow type routing rule and tombstones any
// previously active rule for the same workflow type. The append-only shape preserves enough
// history to roll back and lets older readers ignore the new data.
func SetWorkflowTypeRoutingRule(
	clock *hlc.Clock,
	data *persistencespb.WorkflowTypeRoutingData,
	workflowType string,
	version *deploymentspb.WorkerDeploymentVersion,
	percentage float32,
) (*persistencespb.WorkflowTypeRoutingData, error) {
	if clock == nil {
		return nil, errWorkflowTypeRoutingNoClock
	}
	if workflowType == "" {
		return nil, errWorkflowTypeRoutingEmptyType
	}
	if math.IsNaN(float64(percentage)) || percentage < 0 || percentage > 100 {
		return nil, errWorkflowTypeRoutingInvalidPercent
	}
	if (version.GetDeploymentName() == "") != (version.GetBuildId() == "") {
		return nil, errWorkflowTypeRoutingInvalidVersion
	}

	data = cloneWorkflowTypeRoutingData(data)
	for _, rule := range data.GetRules() {
		if rule.GetWorkflowType() == workflowType && rule.GetDeleteTimestamp() == nil {
			rule.DeleteTimestamp = clock
		}
	}
	var target *deploymentspb.WorkerDeploymentVersion
	if version != nil {
		target = common.CloneProto(version)
	}
	data.Rules = append(data.Rules, &persistencespb.WorkflowTypeRoutingRule{
		WorkflowType:      workflowType,
		DeploymentVersion: target,
		Percentage:        percentage,
		CreateTimestamp:   clock,
	})
	data.RevisionNumber++
	return data, nil
}

// DeleteWorkflowTypeRoutingRule tombstones the active rule for a workflow type.
func DeleteWorkflowTypeRoutingRule(
	clock *hlc.Clock,
	data *persistencespb.WorkflowTypeRoutingData,
	workflowType string,
) (*persistencespb.WorkflowTypeRoutingData, error) {
	if clock == nil {
		return nil, errWorkflowTypeRoutingNoClock
	}
	data = cloneWorkflowTypeRoutingData(data)
	for _, rule := range data.GetRules() {
		if rule.GetWorkflowType() == workflowType && rule.GetDeleteTimestamp() == nil {
			rule.DeleteTimestamp = clock
		}
	}
	data.RevisionNumber++
	return data, nil
}

// RollbackWorkflowTypeRoutingRule tombstones the active rule for a workflow type and restores
// the rule that was active before it. If no previous rule exists, the workflow type falls back to
// the task queue's current/ramping routing.
func RollbackWorkflowTypeRoutingRule(
	clock *hlc.Clock,
	data *persistencespb.WorkflowTypeRoutingData,
	workflowType string,
) (*persistencespb.WorkflowTypeRoutingData, error) {
	if clock == nil {
		return nil, errWorkflowTypeRoutingNoClock
	}

	data = cloneWorkflowTypeRoutingData(data)
	activeIndex := -1
	for i, rule := range data.GetRules() {
		if rule.GetWorkflowType() == workflowType && rule.GetDeleteTimestamp() == nil {
			activeIndex = i
		}
	}
	if activeIndex == -1 {
		return data, nil
	}

	active := data.GetRules()[activeIndex]
	active.DeleteTimestamp = clock
	for i := activeIndex - 1; i >= 0; i-- {
		previous := data.GetRules()[i]
		if previous.GetWorkflowType() != workflowType {
			continue
		}
		if previous.GetDeleteTimestamp() != nil &&
			active.GetCreateTimestamp() != nil &&
			!hlc.Equal(previous.GetDeleteTimestamp(), active.GetCreateTimestamp()) {
			// The previous rule was deleted by an earlier update, not replaced by the active
			// rule. Rolling back across it would resurrect an explicitly deleted rule.
			break
		}
		var target *deploymentspb.WorkerDeploymentVersion
		if previous.GetDeploymentVersion() != nil {
			target = common.CloneProto(previous.GetDeploymentVersion())
		}
		data.Rules = append(data.Rules, &persistencespb.WorkflowTypeRoutingRule{
			WorkflowType:      workflowType,
			DeploymentVersion: target,
			Percentage:        previous.GetPercentage(),
			CreateTimestamp:   clock,
		})
		break
	}
	data.RevisionNumber++
	return data, nil
}

// FindActiveWorkflowTypeRoutingRule returns the last active rule for a workflow type.
func FindActiveWorkflowTypeRoutingRule(
	data *persistencespb.WorkflowTypeRoutingData,
	workflowType string,
) *persistencespb.WorkflowTypeRoutingRule {
	var active *persistencespb.WorkflowTypeRoutingRule
	for _, rule := range data.GetRules() {
		if rule.GetWorkflowType() == workflowType && rule.GetDeleteTimestamp() == nil {
			active = rule
		}
	}
	return active
}

// SelectWorkflowTypeRoutingRule picks the rule target for one workflow execution. It returns
// nil when the execution should use the task queue's normal current/ramping routing.
func SelectWorkflowTypeRoutingRule(
	data *persistencespb.WorkflowTypeRoutingData,
	workflowType string,
	workflowID string,
) *persistencespb.WorkflowTypeRoutingRule {
	rule := FindActiveWorkflowTypeRoutingRule(data, workflowType)
	if rule == nil {
		return nil
	}

	percentage := rule.GetPercentage()
	if percentage <= 0 {
		return nil
	}
	if percentage < 100 {
		threshold := calcWorkflowTypeRampThreshold(workflowType, workflowID)
		if threshold > float64(percentage) {
			return nil
		}
	}
	return rule
}

// FindTargetDeploymentVersionAndRevisionNumberForWorkflowTask applies workflow type routing
// before falling back to the task queue's current/ramping routing.
func FindTargetDeploymentVersionAndRevisionNumberForWorkflowTask(
	current *deploymentspb.WorkerDeploymentVersion,
	currentRevisionNumber int64,
	ramping *deploymentspb.WorkerDeploymentVersion,
	rampingPercentage float32,
	rampingRevisionNumber int64,
	workflowType string,
	workflowID string,
	useRampingVersion bool,
	workflowTypeRouting *persistencespb.WorkflowTypeRoutingData,
) (*deploymentspb.WorkerDeploymentVersion, int64) {
	if rule := SelectWorkflowTypeRoutingRule(workflowTypeRouting, workflowType, workflowID); rule != nil {
		revisionNumber := max(workflowTypeRouting.GetRevisionNumber(), currentRevisionNumber, rampingRevisionNumber)
		return rule.GetDeploymentVersion(), revisionNumber
	}
	return FindTargetDeploymentVersionAndRevisionNumberForWorkflowID(
		current,
		currentRevisionNumber,
		ramping,
		rampingPercentage,
		rampingRevisionNumber,
		workflowID,
		useRampingVersion,
	)
}

func calcWorkflowTypeRampThreshold(workflowType, workflowID string) float64 {
	h := farm.Fingerprint32([]byte(workflowType + "\x00" + workflowID))
	return 100 * (float64(h) / (float64(math.MaxUint32) + 1))
}

func cloneWorkflowTypeRoutingData(data *persistencespb.WorkflowTypeRoutingData) *persistencespb.WorkflowTypeRoutingData {
	if data == nil {
		return &persistencespb.WorkflowTypeRoutingData{}
	}
	return common.CloneProto(data)
}
