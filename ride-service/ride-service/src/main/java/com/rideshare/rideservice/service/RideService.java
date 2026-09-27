package com.rideshare.rideservice.service;

import com.rideshare.rideservice.dto.RideRequest;
import com.rideshare.rideservice.dto.RideResponse;
import com.rideshare.rideservice.event.RideRequestedEvent;
import com.rideshare.rideservice.model.Ride;
import com.rideshare.rideservice.model.RideStatus;
import com.rideshare.rideservice.repository.RideRepository;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class RideService {

    private final RideRepository rideRepository;
    private final KafkaTemplate<String, RideRequestedEvent> kafkaTemplate;
    private static final String RIDE_REQUESTED_TOPIC = "ride.requested";

    public RideResponse requestRide(@Valid RideRequest rideRequest) {
        log.info("New ride requested from ride: {}", rideRequest.getRiderId());

        Ride ride = new Ride();
        ride.setRiderId(rideRequest.getRiderId());
        ride.setPickupLongitude(rideRequest.getPickupLongitude());
        ride.setPickupLatitude(rideRequest.getPickupLatitude());
        ride.setPickupAddress(rideRequest.getPickupAddress());
        ride.setDropLongitude(rideRequest.getDropLongitude());
        ride.setDropLatitude(rideRequest.getDropLatitude());
        ride.setDropAddress(rideRequest.getDropAddress());
        ride.setRideStatus(RideStatus.REQUESTED);
        ride.setEstimatedFare(calculateEstimatedFare(rideRequest));

        Ride saveRide = rideRepository.save(ride);

        RideRequestedEvent event = new RideRequestedEvent(
                saveRide.getId(),
                saveRide.getRiderId(),
                saveRide.getPickupLatitude(),
                saveRide.getPickupLongitude(),
                saveRide.getPickupAddress(),
                saveRide.getDropLatitude(),
                saveRide.getDropLongitude(),
                saveRide.getDropAddress()
        );

        kafkaTemplate.send(RIDE_REQUESTED_TOPIC, saveRide.getId(), event);
        log.info("RideRequestedEvent published to kafka for ride {}", saveRide.getId());

        saveRide.setRideStatus(RideStatus.MATCHING);
        rideRepository.save(saveRide);

        return mapToResponse(saveRide);
    }

    private RideResponse mapToResponse(Ride ride) {
        RideResponse response = new RideResponse();
        response.setId(ride.getId());
        response.setRiderId(ride.getRiderId());
        response.setDriverId(ride.getDriverId());
        response.setPickupLatitude(ride.getPickupLatitude());
        response.setPickupLongitude(ride.getPickupLongitude());
        response.setPickupAddress(ride.getPickupAddress());
        response.setDropLatitude(ride.getDropLatitude());
        response.setDropLongitude(ride.getDropLongitude());
        response.setDropAddress(ride.getDropAddress());
        response.setRideStatus(ride.getRideStatus());
        response.setEstimatedFare(ride.getEstimatedFare());
        response.setActualFare(ride.getActualFare());
        response.setCreatedAt(ride.getCreatedAt());
        response.setStartedAt(ride.getStartedAt());
        response.setCompletedAt(ride.getCompletedAt());
        return response;
    }

    public void updateRideWithDriver(String rideId, String driverId){
        Ride ride = rideRepository.findById(rideId)
                .orElseThrow(() -> new RuntimeException("Ride not found"));

        ride.setDriverId(driverId);
        ride.setRideStatus(RideStatus.ACCEPTED);
        rideRepository.save(ride);
    }

    public RideResponse getRideById(String rideId) {
        Ride ride = rideRepository.findById(rideId)
                .orElseThrow(() -> new RuntimeException("Ride not found"));
        return mapToResponse(ride);
    }

    public List<RideResponse> getRidesByRider(String riderId) {
        return rideRepository.findByRiderIdOrderByCreatedAtDesc(riderId)
                .stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());
    }

    public RideResponse startRide(String rideId) {
        Ride ride = rideRepository.findById(rideId)
                .orElseThrow(() -> new RuntimeException("Ride not found"));

        if(ride.getRideStatus() != RideStatus.ACCEPTED){
            throw new RuntimeException("Ride cannot be started, Current status: " + ride.getRideStatus());
        }

        ride.setRideStatus(RideStatus.RIDE_STARTED);
        ride.setStartedAt(LocalDateTime.now());
        rideRepository.save(ride);
        return mapToResponse(ride);
    }

    public RideResponse completeRide(String rideId) {
        Ride ride = rideRepository.findById(rideId)
                .orElseThrow(() -> new RuntimeException("Ride not found"));

        if(ride.getRideStatus() != RideStatus.RIDE_STARTED){
            throw new RuntimeException("Ride cannot be completed. Current status: " + ride.getRideStatus());
        }

        ride.setRideStatus(RideStatus.COMPLETED);
        ride.setCompletedAt(LocalDateTime.now());
        ride.setActualFare(ride.getEstimatedFare());
        rideRepository.save(ride);
        return mapToResponse(ride);
    }

    public RideResponse cancelRide(String rideId) {
        Ride ride = rideRepository.findById(rideId)
                .orElseThrow(() -> new RuntimeException("Ride not found"));

        ride.setRideStatus(RideStatus.CANCELLED);
        rideRepository.save(ride);
        return mapToResponse(ride);
    }

    private double calculateEstimatedFare(RideRequest request){
        // Simplified Haversine distance calculation
        double lat1 = Math.toRadians(request.getPickupLatitude());
        double lat2 = Math.toRadians(request.getDropLatitude());

        double lon1 = Math.toRadians(request.getPickupLongitude());
        double lon2 = Math.toRadians(request.getDropLongitude());

        double dLat = lat2 - lat1;
        double dLon = lon2 - lon1;

        double a = Math.pow(Math.sin(dLat / 2), 2)
                +Math.cos(lat1) * Math.cos(lat2)
                *Math.pow(Math.sin(dLon / 2), 2);

        double c = 2 * Math.asin(Math.sqrt(a));
        double distanceInKm = 6371 * c;

        //Base fare: 50Rs + 12Rs. perKm
        double fare = 50 + (distanceInKm * 12);
        return Math.round(fare * 100.0) / 100.0;
    }
}
