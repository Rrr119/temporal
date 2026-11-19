package worker_versioning

import (
	"testing"

	"github.com/stretchr/testify/require"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"google.golang.org/protobuf/proto"
)

func routingTestClock(wallClock int64) *hlc.Clock {
	return &hlc.Clock{WallClock: wallClock}
}

func routingTestVersion(deploymentName, buildID string) *deploymentspb.WorkerDeploymentVersion {
	return &deploymentspb.WorkerDeploymentVersion{DeploymentName: deploymentName, BuildId: buildID}
}

func TestSetWorkflowTypeRoutingRule(t *testing.T) {
	version := routingTestVersion("deployment", "build-1")

	tests := []struct {
		name         string
		workflowType string
		version      *deploymentspb.WorkerDeploymentVersion
		percentage   float32
		wantErr      string
	}{
		{name: "empty workflow type", percentage: 100, wantErr: "workflow type cannot be empty"},
		{name: "negative percentage", workflowType: "workflow", version: version, percentage: -1, wantErr: "percentage must be in range"},
		{name: "percentage above 100", workflowType: "workflow", version: version, percentage: 101, wantErr: "percentage must be in range"},
		{name: "missing build ID", workflowType: "workflow", version: routingTestVersion("deployment", ""), percentage: 100, wantErr: "must have both deployment name and build ID"},
		{name: "valid unversioned target", workflowType: "workflow", percentage: 100},
		{name: "valid version target", workflowType: "workflow", version: version, percentage: 10},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := SetWorkflowTypeRoutingRule(routingTestClock(1), nil, tc.workflowType, tc.version, tc.percentage)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				require.Nil(t, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, int64(1), got.GetRevisionNumber())
			rule := FindActiveWorkflowTypeRoutingRule(got, tc.workflowType)
			require.NotNil(t, rule)
			require.Equal(t, tc.percentage, rule.GetPercentage())
			require.True(t, proto.Equal(tc.version, rule.GetDeploymentVersion()))
		})
	}
}

func TestSetWorkflowTypeRoutingRuleReplacesActiveRule(t *testing.T) {
	firstVersion := routingTestVersion("deployment", "build-1")
	secondVersion := routingTestVersion("deployment", "build-2")

	first, err := SetWorkflowTypeRoutingRule(routingTestClock(1), nil, "workflow", firstVersion, 10)
	require.NoError(t, err)
	second, err := SetWorkflowTypeRoutingRule(routingTestClock(2), first, "workflow", secondVersion, 20)
	require.NoError(t, err)

	require.Len(t, second.GetRules(), 2)
	require.NotNil(t, second.GetRules()[0].GetDeleteTimestamp())
	require.Equal(t, int64(2), second.GetRevisionNumber())
	require.True(t, proto.Equal(secondVersion, FindActiveWorkflowTypeRoutingRule(second, "workflow").GetDeploymentVersion()))

	require.Nil(t, first.GetRules()[0].GetDeleteTimestamp(), "input must not be mutated")
	require.Len(t, first.GetRules(), 1)
}

func TestRollbackWorkflowTypeRoutingRule(t *testing.T) {
	firstVersion := routingTestVersion("deployment", "build-1")
	secondVersion := routingTestVersion("deployment", "build-2")

	first, err := SetWorkflowTypeRoutingRule(routingTestClock(1), nil, "workflow", firstVersion, 10)
	require.NoError(t, err)
	second, err := SetWorkflowTypeRoutingRule(routingTestClock(2), first, "workflow", secondVersion, 50)
	require.NoError(t, err)
	rolledBack, err := RollbackWorkflowTypeRoutingRule(routingTestClock(3), second, "workflow")
	require.NoError(t, err)

	require.Equal(t, int64(3), rolledBack.GetRevisionNumber())
	rule := FindActiveWorkflowTypeRoutingRule(rolledBack, "workflow")
	require.Equal(t, float32(10), rule.GetPercentage())
	require.True(t, proto.Equal(firstVersion, rule.GetDeploymentVersion()))
	require.NotNil(t, rolledBack.GetRules()[1].GetDeleteTimestamp())
}

func TestRollbackWorkflowTypeRoutingRuleWithNoPreviousRule(t *testing.T) {
	data, err := SetWorkflowTypeRoutingRule(routingTestClock(1), nil, "workflow", routingTestVersion("deployment", "build-1"), 100)
	require.NoError(t, err)

	rolledBack, err := RollbackWorkflowTypeRoutingRule(routingTestClock(2), data, "workflow")
	require.NoError(t, err)
	require.Nil(t, FindActiveWorkflowTypeRoutingRule(rolledBack, "workflow"))
}

func TestSelectWorkflowTypeRoutingRule(t *testing.T) {
	data, err := SetWorkflowTypeRoutingRule(
		routingTestClock(1),
		nil,
		"workflow",
		routingTestVersion("deployment", "canary"),
		50,
	)
	require.NoError(t, err)

	require.Nil(t, SelectWorkflowTypeRoutingRule(data, "other-workflow", "workflow-id"))

	var selected, unselected int
	for i := range 100 {
		workflowID := "workflow-id-" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		replayed := proto.Clone(data).(*persistencespb.WorkflowTypeRoutingData)
		wantSelected := SelectWorkflowTypeRoutingRule(replayed, "workflow", workflowID) != nil
		if wantSelected {
			selected++
		} else {
			unselected++
		}
		require.Equal(t, wantSelected, SelectWorkflowTypeRoutingRule(data, "workflow", workflowID) != nil,
			"selection must be deterministic for %s", workflowID)
	}
	require.Positive(t, selected)
	require.Positive(t, unselected)
}

func TestFindTargetDeploymentVersionAndRevisionNumberForWorkflowTask(t *testing.T) {
	current := routingTestVersion("deployment", "current")
	ramping := routingTestVersion("deployment", "ramping")
	canary := routingTestVersion("deployment", "canary")

	data, err := SetWorkflowTypeRoutingRule(routingTestClock(1), nil, "workflow", canary, 100)
	require.NoError(t, err)

	tests := []struct {
		name         string
		routing      *persistencespb.WorkflowTypeRoutingData
		workflowType string
		useRamping   bool
		rampPercent  float32
		wantVersion  *deploymentspb.WorkerDeploymentVersion
		wantRevision int64
	}{
		{
			name:         "workflow type rule wins",
			routing:      data,
			workflowType: "workflow",
			wantVersion:  canary,
			wantRevision: 9,
		},
		{
			name:         "other workflow type uses current",
			routing:      data,
			workflowType: "other-workflow",
			wantVersion:  current,
			wantRevision: 7,
		},
		{
			name:         "other workflow type honors ramping",
			routing:      data,
			workflowType: "other-workflow",
			useRamping:   true,
			rampPercent:  100,
			wantVersion:  ramping,
			wantRevision: 9,
		},
		{
			name:         "nil routing data is history compatible",
			workflowType: "workflow",
			wantVersion:  current,
			wantRevision: 7,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotVersion, gotRevision := FindTargetDeploymentVersionAndRevisionNumberForWorkflowTask(
				current,
				7,
				ramping,
				tc.rampPercent,
				9,
				tc.workflowType,
				"workflow-id",
				tc.useRamping,
				tc.routing,
			)
			require.True(t, proto.Equal(tc.wantVersion, gotVersion))
			require.Equal(t, tc.wantRevision, gotRevision)
		})
	}
}

func TestWorkflowTypeRoutingReplaysAcrossSerializedVersions(t *testing.T) {
	oldUserData := &persistencespb.TaskQueueUserData{
		PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
			int32(1): {},
		},
	}
	oldBytes, err := proto.Marshal(oldUserData)
	require.NoError(t, err)

	var replayedOld persistencespb.TaskQueueUserData
	require.NoError(t, proto.Unmarshal(oldBytes, &replayedOld))
	require.Nil(t, replayedOld.GetPerType()[1].GetWorkflowTypeRouting())

	routingData, err := SetWorkflowTypeRoutingRule(
		routingTestClock(1),
		nil,
		"workflow",
		routingTestVersion("deployment", "canary"),
		100,
	)
	require.NoError(t, err)
	newUserData := &persistencespb.TaskQueueUserData{
		PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
			int32(1): {WorkflowTypeRouting: routingData},
		},
	}
	newBytes, err := proto.Marshal(newUserData)
	require.NoError(t, err)

	var replayedNew persistencespb.TaskQueueUserData
	require.NoError(t, proto.Unmarshal(newBytes, &replayedNew))

	workflowIDs := []string{"one", "two", "three"}
	for _, workflowID := range workflowIDs {
		oldVersion, oldRevision := FindTargetDeploymentVersionAndRevisionNumberForWorkflowTask(
			routingTestVersion("deployment", "current"),
			3,
			nil,
			0,
			0,
			"workflow",
			workflowID,
			false,
			replayedOld.GetPerType()[1].GetWorkflowTypeRouting(),
		)
		newVersion, newRevision := FindTargetDeploymentVersionAndRevisionNumberForWorkflowTask(
			routingTestVersion("deployment", "current"),
			3,
			nil,
			0,
			0,
			"workflow",
			workflowID,
			false,
			replayedNew.GetPerType()[1].GetWorkflowTypeRouting(),
		)
		require.Equal(t, "current", oldVersion.GetBuildId(), "old data keeps old routing")
		require.Equal(t, int64(3), oldRevision)
		require.Equal(t, "canary", newVersion.GetBuildId(), "new data applies the canary")
		require.Equal(t, int64(3), newRevision)
	}
}
