/*
 * Copyright (c) 2023 - Restate Software, Inc., Restate GmbH
 *
 * This file is part of the Restate SDK for Node.js/TypeScript,
 * which is released under the MIT license.
 *
 * You can find a copy of the license in file LICENSE in the root
 * directory of this repository or package, or at
 * https://github.com/restatedev/sdk-typescript/blob/main/LICENSE
 */

import * as restate from "@restatedev/restate-sdk";
import { TerminalError } from "@restatedev/restate-sdk";
import { carRentalService } from "./cars";
import { flightsService } from "./flights";
import { paymentsService } from "./payments";
import { PublishCommand, SNSClient } from "@aws-sdk/client-sns";
import * as process from "process";

const sns = new SNSClient({ endpoint: process.env.AWS_ENDPOINT });

const reserve = async (ctx: restate.RpcContext, request?: { run_type?: string; trip_id?: string }) => {
  console.log("reserve trip:", JSON.stringify(request, undefined, 2));

  const trip_id = request?.trip_id ?? ctx.rand.uuidv4();

  let input = {
    trip_id,
    depart_city: "Detroit",
    depart_time: "2021-07-07T06:00:00.000Z",
    arrive_city: "Frankfurt",
    arrive_time: "2021-07-09T08:00:00.000Z",
    rental: "BMW",
    rental_from: "2021-07-09T00:00:00.000Z",
    rental_to: "2021-07-17T00:00:00.000Z",
    run_type: request?.run_type,
  };

  let flight_booking, car_booking, payment;

  try {
    // call the flights Lambda to reserve, keeping track of how to cancel
    flight_booking = await ctx.rpc(flightsService).reserve(trip_id, input);

    // RPC the rental service to reserve, keeping track of how to cancel
    const car_booking = await ctx.rpc(carRentalService).reserve(trip_id, input);

    // RPC the payments service to process, keeping track of how to refund
    payment = await ctx.rpc(paymentsService).process(trip_id, { run_type: input.run_type });

    // confirm the flight and car
    await ctx.rpc(flightsService).confirm(trip_id, flight_booking);
    await ctx.rpc(carRentalService).confirm(trip_id, car_booking);

    // simulate a failing SNS call
    if (request?.run_type === "failNotification") {
      await Promise.reject(new TerminalError("Failed to send notification"));
    }
  } catch (error) {
    // asynchronously refund the payment (if one was authorized)
    if (payment) ctx.send(paymentsService).refund(trip_id, payment);

    // cancel the flight and car (if reservations were made)
    if (car_booking) await ctx.rpc(carRentalService).cancel(trip_id, car_booking);
    if (flight_booking) await ctx.rpc(flightsService).cancel(trip_id, flight_booking);

    throw new TerminalError(`Reservation failed: '${error}'`, { cause: error });
  }

  // notify failure
  await ctx.sideEffect(() => sns.send(new PublishCommand({
    TopicArn: process.env.SNS_TOPIC,
    Message: "Your Travel Reservation Failed",
  })));

  return { status: "SUCCESS", id: trip_id };
};

export const tripsRouter = restate.router({ reserve });
export const tripsService: restate.ServiceApi<typeof tripsRouter> = { path: "trips" };

export const handler = restate.createLambdaApiGatewayHandler().bindRouter(tripsService.path, tripsRouter).handle();
