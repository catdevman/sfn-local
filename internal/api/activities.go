package api

import (
	"context"
	"sort"

	"github.com/catdevman/sfn-local/internal/api/gen"
	"github.com/catdevman/sfn-local/internal/store"
)

func (s *Service) CreateActivity(_ context.Context, in *gen.CreateActivityInput) (*gen.CreateActivityOutput, error) {
	name := ""
	if in != nil && in.Name != nil {
		name = *in.Name
	}
	if name == "" || NameRejected(name) {
		return nil, modeled("InvalidName", "Invalid Name: '"+name+"'")
	}
	if in.EncryptionConfiguration != nil {
		return nil, modeled("InvalidEncryptionConfiguration", "encryptionConfiguration is not supported by this local server.")
	}
	tags, err := tagsFrom(in.Tags)
	if err != nil {
		return nil, err
	}
	activity := store.Activity{
		ARN:     ActivityARN(s.region(), s.account(), name),
		Name:    name,
		Created: s.now(),
	}
	stored, err := s.Store.PutActivity(activity, tags)
	if err != nil {
		if exists, ok := err.(*store.ExistsError); ok {
			return nil, modeled("ActivityAlreadyExists", "Activity Already Exists: '"+exists.ARN+"'")
		}
		return nil, err
	}
	return &gen.CreateActivityOutput{ActivityArn: sp(stored.ARN), CreationDate: ts(stored.Created)}, nil
}

func (s *Service) DescribeActivity(_ context.Context, in *gen.DescribeActivityInput) (*gen.DescribeActivityOutput, error) {
	a, err := s.activityByARN(valueOf(in.ActivityArn))
	if err != nil {
		return nil, err
	}
	return &gen.DescribeActivityOutput{
		ActivityArn:  sp(a.ARN),
		Name:         sp(a.Name),
		CreationDate: ts(a.Created),
	}, nil
}

func (s *Service) ListActivities(_ context.Context, in *gen.ListActivitiesInput) (*gen.ListActivitiesOutput, error) {
	rows := s.Store.Activities()
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })
	var max *int32
	var token *string
	if in != nil {
		max, token = in.MaxResults, in.NextToken
	}
	start, end, next, err := pageBounds(len(rows), max, token)
	if err != nil {
		return nil, err
	}
	list := make(gen.ActivityList, 0, end-start)
	for _, a := range rows[start:end] {
		list = append(list, gen.ActivityListItem{
			ActivityArn:  sp(a.ARN),
			Name:         sp(a.Name),
			CreationDate: ts(a.Created),
		})
	}
	return &gen.ListActivitiesOutput{Activities: &list, NextToken: next}, nil
}

func (s *Service) DeleteActivity(_ context.Context, in *gen.DeleteActivityInput) (*gen.DeleteActivityOutput, error) {
	if _, err := s.activityByARN(valueOf(in.ActivityArn)); err != nil {
		return nil, err
	}
	s.Store.DeleteActivity(*in.ActivityArn)
	return &gen.DeleteActivityOutput{}, nil
}

func (s *Service) activityByARN(arn string) (store.Activity, error) {
	if !ValidActivityARN(arn) {
		return store.Activity{}, modeled("InvalidArn", "Invalid Arn: '"+arn+"'")
	}
	a, ok := s.Store.Activity(arn)
	if !ok {
		return store.Activity{}, modeled("ActivityDoesNotExist", "Activity Does Not Exist: '"+arn+"'")
	}
	return a, nil
}

func valueOf(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func sortMachines(rows []store.Machine) {
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })
}
