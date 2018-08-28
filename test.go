package processor

import (
	"errors"
	"fmt"
	Interfaces "github.com/gitalytics/alerts/pkg/interface/scope"
	"github.com/gitalytics/alerts/pkg/model"
	"github.com/gitalytics/alerts/pkg/util"
	"github.com/gitalytics/messenger/payload"
	"strconv"
	"time"
)

const ActiveAlertTime = 86400 // 60*60*24 seconds

func ChurnRefactor(payload *payload.AlertPayload, scope Interfaces.Scope) error {
	config, err := scope.DB().AlertConfig.GetByType(model.ChurnOrRefactor)
	if err != nil {
		return err
	}

	if config == nil {
		return errors.New("could not find configuration for the alert type " + model.CommitSize)

	}

	// This particular is disabled, skip it
	if !config.Active {
		return nil
	}

	commit, err := scope.DB().Commit.Get(payload.EntityID)
	if err != nil {
		return err
	}

	if IsOutdatedAlert(commit.Date) {
		return nil
	}

	endDate := time.Unix(commit.Date/1000, 0).UTC()
	startDate := util.GetIntervalStartDate(endDate, config.IntervalType).Add(time.Second)

	result, err := scope.DB().Commit.GetByHighChurnRefactor(startDate, endDate, config.Threshold, commit.ContributorID, commit.RepositoryID)
	if err != nil {
		return err
	}

	if result != nil {
		result.TriggerDate = commit.Date

		alert := model.NewRefactorChurnAlert(config, result)
		if err := scope.DB().Alert.Create(alert); err != nil {
			return err
		}

		// Nothing to report about, if alert document already exists
		// However, we want to continue processing with the rest of the alert types
		if alert != nil {
			message := fmt.Sprintf(
				"%v %v's commits have been %s%s",
				*result.Contributor.FirstName,
				*result.Contributor.LastName,
				strconv.FormatInt(alert.Value, 10),
				"% churn over the past week",
			)

			if err := scope.Messenger().SendMessage(message, "Worktype", alert.ID); err != nil {
				return err
			}
		}
	}

	return nil
}

func IsOutdatedAlert(timestamp int64) bool {
	return (time.Now().UTC().Unix() - timestamp/1000) > ActiveAlertTime
}
