package main

import (
	"data_access/model"
	"fmt"

	"github.com/siddontang/go-mysql/canal"
	"go.uber.org/zap"
)

type handler struct {
	canal.DummyEventHandler
}

func (h *handler) OnRow(e *canal.RowsEvent) error {
	fmt.Printf("%v\n", e)
	var err error

	switch e.Action {
	case canal.UpdateAction:
		err = handleUpdateAction(e)
	case canal.InsertAction:
	case canal.DeleteAction:
	default:
		zapper.Error("未知动作", zap.Any("action", e.Action))
		return fmt.Errorf("未知动作")
	}

	return err
}

func (h *handler) String() string {
	return "TestHandler"
}

func handleUpdateAction(e *canal.RowsEvent) error {
	fmt.Println(e.Table.PKColumns)
	fmt.Println(e.Table.GetPKColumn(0))

	rows := make([][]*model.Cell, 0, 128)
	for i, v := range e.Rows {
		if i%2 == 1 {
			continue
		}

		pkCells := make([]*model.Cell, 0, 2)
		for _, v2 := range e.Table.PKColumns {
			tmp := e.Table.GetPKColumn(v2)
			tmp2 := &model.Cell{
				Name:  tmp.Name,
				Value: v[v2],
			}

			pkCells = append(pkCells, tmp2)
		}
		rows = append(rows, pkCells)
	}

	fmt.Println(rows)
	for _, v := range rows {
		for _, v2 := range v {
			fmt.Println(v2)
		}
	}

	return nil
}

func handleInsertAction(e *canal.RowsEvent) error {
	return nil
}

func handleDeleteAction(e *canal.RowsEvent) error {
	return nil
}
