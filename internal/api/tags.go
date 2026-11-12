package api

import (
	"context"

	"github.com/catdevman/sfn-local/internal/api/gen"
	"github.com/catdevman/sfn-local/internal/store"
)

func (s *Service) TagResource(_ context.Context, in *gen.TagResourceInput) (*gen.TagResourceOutput, error) {
	arn, err := s.tagResource(valueOf(in.ResourceArn))
	if err != nil {
		return nil, err
	}
	tags, err := tagsFrom(in.Tags)
	if err != nil {
		return nil, err
	}
	if _, kind := s.Store.AddTags(arn, tags); kind != store.TagMergeOK {
		if kind == store.TagMergeLimit {
			return nil, modeled("TooManyTags", "Too many tags provided.")
		}
		return nil, modeled("ResourceNotFound", "Resource not found: '"+arn+"'")
	}
	return &gen.TagResourceOutput{}, nil
}

func (s *Service) UntagResource(_ context.Context, in *gen.UntagResourceInput) (*gen.UntagResourceOutput, error) {
	arn, err := s.tagResource(valueOf(in.ResourceArn))
	if err != nil {
		return nil, err
	}
	var keys []string
	if in.TagKeys != nil {
		keys = append(keys, (*in.TagKeys)...)
	}
	if !s.Store.RemoveTags(arn, keys) {
		return nil, modeled("ResourceNotFound", "Resource not found: '"+arn+"'")
	}
	return &gen.UntagResourceOutput{}, nil
}

func (s *Service) ListTagsForResource(_ context.Context, in *gen.ListTagsForResourceInput) (*gen.ListTagsForResourceOutput, error) {
	arn, err := s.tagResource(valueOf(in.ResourceArn))
	if err != nil {
		return nil, err
	}
	tags, ok := s.Store.Tags(arn)
	if !ok {
		return nil, modeled("ResourceNotFound", "Resource not found: '"+arn+"'")
	}
	if tags == nil {
		tags = []store.Tag{}
	}
	return &gen.ListTagsForResourceOutput{Tags: toGenTags(tags)}, nil
}

func (s *Service) tagResource(arn string) (string, error) {
	if !ValidStateMachineARN(arn) && !ValidActivityARN(arn) {
		return "", modeled("InvalidArn", "Invalid Arn: '"+arn+"'")
	}
	if _, ok := s.Store.Tags(arn); !ok {
		return "", modeled("ResourceNotFound", "Resource not found: '"+arn+"'")
	}
	return arn, nil
}
