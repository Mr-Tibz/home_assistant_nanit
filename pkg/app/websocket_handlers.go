package app

import (
	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/indiefan/home_assistant_nanit/pkg/client"
)

func processSensorData(babyUID string, sensorData []*client.SensorData, stateManager *baby.StateManager) {
	// Parse sensor update
	stateUpdate := baby.State{}
	for _, sensorDataSet := range sensorData {
		// A reading without its type or its value is left out. Reading
		// such a field without a check ended the program.
		if sensorDataSet.SensorType == nil {
			continue
		}

		switch *sensorDataSet.SensorType {
		case client.SensorType_TEMPERATURE:
			if sensorDataSet.ValueMilli != nil {
				stateUpdate.SetTemperatureMilli(*sensorDataSet.ValueMilli)
			}
		case client.SensorType_HUMIDITY:
			if sensorDataSet.ValueMilli != nil {
				stateUpdate.SetHumidityMilli(*sensorDataSet.ValueMilli)
			}
		case client.SensorType_NIGHT:
			if sensorDataSet.Value != nil {
				stateUpdate.SetIsNight(*sensorDataSet.Value == 1)
			}
		}
	}

	stateManager.Update(babyUID, stateUpdate)
}

func processLight(babyUID string, control *client.Control, stateManager *baby.StateManager) {
	if control.NightLight != nil {
		stateUpdate := baby.State{}
		stateUpdate.SetNightLight(*control.NightLight == client.Control_LIGHT_ON)
		stateManager.Update(babyUID, stateUpdate)
	}
}

func sendLightCommand(nightLightState bool, conn *client.WebsocketConnection) {
	nightLight := client.Control_LIGHT_OFF
	if nightLightState {
		nightLight = client.Control_LIGHT_ON
	}
	conn.SendRequest(client.RequestType_PUT_CONTROL, &client.Request{
		Control: &client.Control{
			NightLight: &nightLight,
		},
	})
}

func processStandby(babyUID string, settings *client.Settings, stateManager *baby.StateManager) {
	if settings.SleepMode != nil {
		stateUpdate := baby.State{}
		stateUpdate.SetStandby(*settings.SleepMode)
		stateManager.Update(babyUID, stateUpdate)
	}
}

func sendStandbyCommand(standbyState bool, conn *client.WebsocketConnection) {
	conn.SendRequest(client.RequestType_PUT_SETTINGS, &client.Request{
		Settings: &client.Settings{
			SleepMode: &standbyState,
		},
	})
}
